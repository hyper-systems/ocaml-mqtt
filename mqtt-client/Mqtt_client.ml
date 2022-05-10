let fmt = Format.asprintf

type connection = Lwt_io.input_channel * Lwt_io.output_channel

let decode_length inch =
  let rec loop value mult =
    let%lwt ch = Lwt_io.read_char inch in
    let ch = Char.code ch in
    let digit = ch land 127 in
    let value = value + (digit * mult) in
    let mult = mult * 128 in
    if ch land 128 = 0 then Lwt.return value else loop value mult
  in
  loop 0 1

let read_packet inch =
  let%lwt header_byte = Lwt_io.read_char inch in
  let msgid, opts =
    Mqtt_packet.Decoder.decode_fixed_header (Char.code header_byte)
  in
  let%lwt count = decode_length inch in

  let data = Bytes.create count in
  let%lwt () =
    try Lwt_io.read_into_exactly inch data 0 count
    with End_of_file -> Lwt.fail (Failure "could not read bytes")
  in
  let pkt =
    Read_buffer.make (data |> Bytes.to_string)
    |> Mqtt_packet.Decoder.decode_packet opts msgid
  in
  Lwt.return (opts, pkt)

module Log = (val Logs_lwt.src_log (Logs.Src.create "mqtt.client"))

type t = {
  cxn : connection;
  id : string;
  inflight : (int, unit Lwt_condition.t * Mqtt_packet.t) Hashtbl.t;
  mutable reader : unit Lwt.t;
  on_message : topic:string -> string -> unit Lwt.t;
  on_disconnect : t -> unit Lwt.t;
  on_error : t -> exn -> unit Lwt.t;
  should_stop_reader : unit Lwt_condition.t;
}

let wrap_catch client f = Lwt.catch f (client.on_error client)

let default_on_error client exn =
  let%lwt () =
    Log.err (fun log ->
        log "[%s]: Unhandled exception: %a" client.id Fmt.exn exn)
  in
  Lwt.fail exn

let default_on_message ~topic:_ _ = Lwt.return_unit
let default_on_disconnect _ = Lwt.return_unit

let read_packets client =
  let in_chan, out_chan = client.cxn in

  let ack_inflight id pkt =
    try
      let cond, expected_ack_pkt = Hashtbl.find client.inflight id in
      if pkt = expected_ack_pkt then (
        Hashtbl.remove client.inflight id;
        Lwt_condition.signal cond ();
        Lwt.return_unit)
      else Lwt.fail (Failure "unexpected packet in ack")
    with Not_found -> Lwt.fail (Failure (fmt "ack for id=%d not found" id))
  in

  let rec loop () =
    let%lwt (_dup, qos, _retain), packet = read_packet in_chan in
    let%lwt () =
      match packet with
      (* Publish with QoS 0: push *)
      | Publish (None, topic, payload) when qos = Atmost_once ->
        client.on_message ~topic payload
      (* Publish with QoS 0 and packet identifier: error *)
      | Publish (Some _id, _topic, _payload) when qos = Atmost_once ->
        Lwt.fail
          (Failure
             "protocol violation: publish packet with qos 0 must not have id")
      (* Publish with QoS 1 *)
      | Publish (Some id, topic, payload) when qos = Atleast_once ->
        (* - Push the message to the consumer queue.
           - Send back the PUBACK packet. *)
        let%lwt () = client.on_message ~topic payload in
        let puback = Mqtt_packet.Encoder.puback id in
        Lwt_io.write out_chan puback
      | Publish (None, _topic, _payload) when qos = Atleast_once ->
        Lwt.fail
          (Failure
             "protocol violation: publish packet with qos > 0 must have id")
      | Publish _ ->
        Lwt.fail (Failure "not supported publish packet (probably qos 2)")
      | Suback (id, _)
      | Unsuback id
      | Puback id
      | Pubrec id
      | Pubrel id
      | Pubcomp id ->
        ack_inflight id packet
      | Pingresp -> Lwt.return_unit
      | _ -> Lwt.fail (Failure "unknown packet from server")
    in
    loop ()
  in

  let%lwt () =
    Log.debug (fun log -> log "[%s] Starting reader loop..." client.id)
  in
  Lwt.pick
    [
      (let%lwt () = Lwt_condition.wait client.should_stop_reader in
       Log.info (fun log -> log "[%s] Stopping reader loop..." client.id));
      loop ();
    ]

let disconnect client =
  let%lwt () =
    Log.info (fun log -> log "[%s] Disconnecting client..." client.id)
  in
  let _, oc = client.cxn in
  Lwt_condition.signal client.should_stop_reader ();
  let%lwt () = Lwt_io.write oc (Mqtt_packet.Encoder.disconnect ()) in
  let%lwt () = client.on_disconnect client in
  Log.info (fun log -> log "[%s] Client disconnected." client.id)

let shutdown client =
  let%lwt () =
    Log.debug (fun log -> log "[%s] Shutting down the connection..." client.id)
  in
  let ic, oc = client.cxn in
  let%lwt () = Lwt_io.flush oc in
  let%lwt () = Lwt_io.close ic in
  let%lwt () = Lwt_io.close oc in
  Log.debug (fun log -> log "[%s] Client connection shut down." client.id)

let open_tls_connection ~client_id ~ca_file host port =
  try%lwt
    let%lwt authenticator = X509_lwt.authenticator (`Ca_file ca_file) in
    Tls_lwt.connect authenticator (host, port)
  with exn ->
    let%lwt () =
      Log.err (fun log ->
          log "[%s] could not get address info for %S" client_id host)
    in
    Lwt.fail exn

let run_pinger ~keep_alive client =
  let%lwt () = Log.debug (fun log -> log "Starting ping timer...") in
  let _, output = client.cxn in
  (* 25% leeway *)
  let keep_alive = 0.75 *. float_of_int keep_alive in
  let rec loop () =
    let%lwt () = Lwt_unix.sleep keep_alive in
    let pingreq_packet = Mqtt_packet.Encoder.pingreq () in
    let%lwt () = Lwt_io.write output pingreq_packet in
    loop ()
  in
  loop ()

exception Connection_error

let open_tcp_connection ~client_id host port =
  let%lwt addresses = Lwt_unix.getaddrinfo host (string_of_int port) [] in
  match addresses with
  | address :: _ ->
    let sockaddr = Lwt_unix.(address.ai_addr) in
    Lwt_io.open_connection sockaddr
  | _ ->
    let%lwt () =
      Log.err (fun log ->
          log "[%s] could not get address info for %S" client_id host)
    in
    Lwt.fail Connection_error

let rec create_connection ?tls_ca ~port ~client_id hosts =
  match hosts with
  | [] ->
    let%lwt () =
      Log.err (fun log ->
          log "[%s] Could not connect to any of the hosts (on port %d): %a"
            client_id port
            Fmt.Dump.(list string)
            hosts)
    in
    Lwt.fail Connection_error
  | host :: hosts -> (
    try%lwt
      let%lwt () =
        Log.debug (fun log ->
            log "[%s] Connecting to `%s:%d`..." client_id host port)
      in
      let%lwt connection =
        match tls_ca with
        | Some ca_file -> open_tls_connection ~client_id ~ca_file host port
        | None -> open_tcp_connection ~client_id host port
      in
      let%lwt () =
        Log.info (fun log ->
            log "[%s] Connection opened on `%s:%d`." client_id host port)
      in
      Lwt.return connection
    with _ ->
      let%lwt () =
        Log.debug (fun log ->
            log "[%s] Could not connect, trying next host..." client_id)
      in
      create_connection ?tls_ca ~port ~client_id hosts)

let connect ?(id = "ocaml-mqtt") ?tls_ca ?credentials ?will
    ?(clean_session = true) ?(keep_alive = 30)
    ?(on_message = default_on_message) ?(on_disconnect = default_on_disconnect)
    ?(on_error = default_on_error) ?(port = 1883) hosts =
  let flags =
    if clean_session || id = "" then [ Mqtt_packet.Clean_session ] else []
  in
  let cxn_data =
    { Mqtt_packet.clientid = id; credentials; will; flags; keep_alive }
  in

  let%lwt ((ic, oc) as connection) =
    create_connection ?tls_ca ~port ~client_id:id hosts
  in

  let connect_packet =
    Mqtt_packet.Encoder.connect ?credentials:cxn_data.credentials
      ?will:cxn_data.will ~flags:cxn_data.flags ~keep_alive:cxn_data.keep_alive
      cxn_data.clientid
  in
  let%lwt () = Lwt_io.write oc connect_packet in
  let inflight = Hashtbl.create 16 in

  match%lwt read_packet ic with
  | _, Connack { connection_status = Accepted; session_present } ->
    let%lwt () =
      Log.debug (fun log ->
          log "[%s] Connection acknowledged (session_present=%b)" id
            session_present)
    in

    let client =
      {
        cxn = connection;
        id;
        inflight;
        reader = Lwt.return_unit;
        should_stop_reader = Lwt_condition.create ();
        on_message;
        on_disconnect;
        on_error;
      }
    in

    Lwt.async (fun () ->
        client.reader <- wrap_catch client (fun () -> read_packets client);
        let%lwt () =
          Log.debug (fun log -> log "[%s] Packet reader started." client.id)
        in
        let%lwt () =
          Lwt.pick [ client.reader; run_pinger ~keep_alive client ]
        in
        let%lwt () =
          Log.debug (fun log ->
              log "[%s] Packet reader stopped, shutting down..." client.id)
        in
        shutdown client);

    Lwt.return client
  | _, Connack pkt ->
    let conn_status =
      Mqtt_packet.connection_status_to_string pkt.connection_status
    in
    let%lwt () =
      Log.err (fun log -> log "[%s] Connection failed: %s" id conn_status)
    in
    Lwt.fail Connection_error
  | _ ->
    let%lwt () =
      Log.err (fun log ->
          log "[%s] Invalid response from broker on connection" id)
    in
    Lwt.fail Connection_error

let publish ?(dup = false) ?(qos = Mqtt_core.Atleast_once) ?(retain = false)
    ~topic payload client =
  let _, oc = client.cxn in
  match qos with
  | Atmost_once ->
    let pkt_data =
      Mqtt_packet.Encoder.publish ~dup ~qos ~retain ~id:0 ~topic payload
    in
    Lwt_io.write oc pkt_data
  | Atleast_once ->
    let id = Mqtt_packet.gen_id () in
    let cond = Lwt_condition.create () in
    let expected_ack_pkt = Mqtt_packet.puback id in
    Hashtbl.add client.inflight id (cond, expected_ack_pkt);
    let pkt_data =
      Mqtt_packet.Encoder.publish ~dup ~qos ~retain ~id ~topic payload
    in
    let%lwt () = Lwt_io.write oc pkt_data in
    Lwt_condition.wait cond
  | Exactly_once ->
    let id = Mqtt_packet.gen_id () in
    let cond = Lwt_condition.create () in
    let expected_ack_pkt = Mqtt_packet.pubrec id in
    Hashtbl.add client.inflight id (cond, expected_ack_pkt);
    let pkt_data =
      Mqtt_packet.Encoder.publish ~dup ~qos ~retain ~id ~topic payload
    in
    let%lwt () = Lwt_io.write oc pkt_data in
    let%lwt () = Lwt_condition.wait cond in
    let expected_ack_pkt = Mqtt_packet.pubcomp id in
    Hashtbl.add client.inflight id (cond, expected_ack_pkt);
    let pkt_data = Mqtt_packet.Encoder.pubrel id in
    let%lwt () = Lwt_io.write oc pkt_data in
    Lwt_condition.wait cond

let subscribe topics client =
  if topics = [] then raise (Invalid_argument "empty topics");
  let _, oc = client.cxn in
  let pkt_id = Mqtt_packet.gen_id () in
  let subscribe_packet = Mqtt_packet.Encoder.subscribe ~id:pkt_id topics in
  let qos_list = List.map (fun (_, q) -> Ok q) topics in
  let cond = Lwt_condition.create () in
  Hashtbl.add client.inflight pkt_id (cond, Suback (pkt_id, qos_list));
  wrap_catch client (fun () ->
      let%lwt () = Lwt_io.write oc subscribe_packet in
      let%lwt () = Lwt_condition.wait cond in
      let topics = List.map fst topics in
      Log.info (fun log ->
          log "[%s] Subscribed to %a." client.id Fmt.Dump.(list string) topics))

include Mqtt_core
