open Lwt

let ( <&> ) = Lwt.( <&> )
let fmt = Format.asprintf

include Mqtt_core

type connection = Lwt_io.input_channel * Lwt_io.output_channel

let decode_length inch =
  let rec loop value mult =
    Lwt_io.read_char inch >>= fun ch ->
    let ch = Char.code ch in
    let digit = ch land 127 in
    let value = value + (digit * mult) in
    let mult = mult * 128 in
    if ch land 128 = 0 then Lwt.return value else loop value mult
  in
  loop 0 1


let read_packet inch =
  Lwt_io.read_char inch >>= fun header_byte ->
  let msgid, opts =
    Mqtt_packet.Decoder.decode_fixed_header (Char.code header_byte)
  in
  decode_length inch >>= fun count ->
  let data = Bytes.create count in
  let rd =
    try Lwt_io.read_into_exactly inch data 0 count
    with End_of_file -> Lwt.fail (Failure "could not read bytes")
  in
  rd >>= fun () ->
  let pkt =
    Read_buffer.make (data |> Bytes.to_string)
    |> Mqtt_packet.Decoder.decode_packet opts msgid
  in
  Lwt.return (opts, pkt)


module Client = struct
  let src = Logs.Src.create "mqtt.client" ~doc:"MQTT Client module"
  module Log = (val Logs_lwt.src_log src : Logs_lwt.LOG)

  type t = {
    cxn : connection;
    inflight : (int, unit Lwt_condition.t * Mqtt_packet.t) Hashtbl.t;
    mutable reader : unit Lwt.t;
    mutable pinger : unit Lwt.t;
    on_message : topic:string -> string -> unit Lwt.t;
    on_disconnect : t -> unit Lwt.t;
    on_error : t -> exn -> unit Lwt.t;
    reset_ping_timer : unit Lwt_mvar.t;
    should_stop_reader : unit Lwt_condition.t;
  }

  let pp_inflight out inflight =
    Fmt.pf out "(inflight";
    Hashtbl.iter (fun msg_id _ -> Fmt.pf out " %d" msg_id) inflight;
    Fmt.pf out ")"


  let default_on_error _client exn =
    let%lwt () =
      Log.err (fun log -> log "Mqtt_client: unhandled client error")
    in
    Lwt.fail exn

  
  let default_on_message ~topic:_ _ =
    Lwt.return_unit


  let default_on_disconnect _ =
    Lwt.return_unit


  let read_packets client () =
    let in_chan, out_chan = client.cxn in

    let ack_inflight id pkt =
      try
        let cond, expected_ack_pkt = Hashtbl.find client.inflight id in
        if pkt = expected_ack_pkt then begin
          Hashtbl.remove client.inflight id;
          Lwt_condition.signal cond ();
          Lwt.return_unit
        end
        else Lwt.fail (Failure "unexpected packet in ack")
      with Not_found -> Lwt.fail (Failure (fmt "ack for id=%d not found" id))
    in

    let _push_id id pkt_data topic payload =
      ack_inflight id pkt_data >>= fun () -> client.on_message ~topic payload
    in

    let rec loop () =
      read_packet in_chan >>= fun ((_dup, qos, _retain), packet) ->
      begin
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
          client.on_message ~topic payload >>= fun () ->
          let puback = Mqtt_packet.Encoder.puback id in
          Lwt_io.write out_chan puback >>= fun () -> Lwt.return_unit
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
        | Pubcomp id -> ack_inflight id packet
        | Pingresp ->
          let%lwt () =
            Log.debug (fun log ->
                if not (Lwt_mvar.is_empty client.reset_ping_timer) then
                  log "Reset ping timer mvar is not empty, will block."
                else Lwt.return_unit)
          in

          Lwt_mvar.put client.reset_ping_timer ()
        | _ -> Lwt.fail (Failure "unknown packet from server")
      end
      >>= fun () -> loop ()
    in
    let%lwt () = Log.debug (fun log -> log "Starting reader loop...") in
    Lwt.pick
      [
        ( Lwt_condition.wait client.should_stop_reader >>= fun () ->
          Log.info (fun log -> log "Stopping reader loop...") );
        loop ();
      ]


  let wrap_catch client f = Lwt.catch f (client.on_error client)

  exception Ping_timeout

  let disconnect client =
    let%lwt () = Log.info (fun log -> log "Disconnecting client...") in
    let _, oc = client.cxn in

    (* Stop reading packets. *)
    Lwt_condition.signal client.should_stop_reader ();

    (* Send the disconnect packet to server. *)
    let%lwt () = Lwt_io.write oc (Mqtt_packet.Encoder.disconnect ()) in

    let%lwt () = client.on_disconnect client in

    Log.info (fun log -> log "Did disconnect client...")


  let shutdown client =
    let%lwt () = Log.debug (fun log -> log "Shutting down the connection...") in
    let ic, oc = client.cxn in
    let%lwt () = Lwt_io.flush oc in
    let%lwt () = Lwt_io.close ic in
    let%lwt () = Lwt_io.close oc in
    Log.debug (fun log -> log "Did shut down the connection.")


  let ping_timer client ?(ping_timeout = 5.0) ~keep_alive () =
    let%lwt () = Log.debug (fun log -> log "Starting ping timer...") in
    let _, output = client.cxn in
    let keep_alive = 0.9 *. float_of_int keep_alive in
    (* 10% leeway *)
    let rec loop () =
      (* Wait for keep alive interval. *)
      let%lwt () =
        Log.debug (fun log -> log "Waiting for keep_alive interval...")
      in
      let%lwt () = Lwt_unix.sleep keep_alive in

      (* Send PINGREQ. *)
      let pingreq_packet = Mqtt_packet.Encoder.pingreq () in
      let%lwt () = Lwt_io.write output pingreq_packet in
      let%lwt () = Log.debug (fun log -> log "Sent ping.") in

      let timeout =
        let%lwt () = Lwt_unix.sleep ping_timeout in
        let%lwt () = Log.debug (fun log -> log "Ping request timed out. ") in
        let%lwt () = disconnect client in
        client.on_error client Ping_timeout
      in

      let reset =
        let%lwt () = Lwt_mvar.take client.reset_ping_timer in
        let%lwt () = Log.debug (fun log -> log "Cancelling timeout... ") in
        Lwt.cancel timeout;
        loop ()
      in
      Lwt.catch
        (fun () -> Lwt.choose [ timeout; reset ])
        (function
          | Lwt.Canceled -> Log.debug (fun log -> log "Did cancel timeout. ")
          | exn -> Lwt.fail exn)
    in
    loop ()


  let () = Printexc.record_backtrace true

  let open_tls_connection ~ca_file host port =
    let%lwt authenticator = X509_lwt.authenticator (`Ca_file ca_file) in
    Tls_lwt.connect authenticator (host, port)


  exception Connection_error of string

  let open_tcp_connection host port =
    Lwt_unix.getaddrinfo host (string_of_int port) [] >>= fun addresses ->
    match addresses with
    | address :: _ ->
      let sockaddr = Lwt_unix.(address.ai_addr) in
      Lwt_io.open_connection sockaddr
    | _ ->
      Lwt.fail
        (Connection_error ("Error: mqtt: could not get address info for " ^ host))


  let connect ?(id = "OCamlMQTT") ?tls_ca ?credentials ?will
      ?(clean_session = true) ?(keep_alive = 10) ?(ping_timeout = 5.0)
      ?(on_message = default_on_message)
      ?(on_disconnect = default_on_disconnect)
      ?(on_error = default_on_error) ?(port = 1883) hosts =
    let flags = if clean_session then [ Mqtt_packet.Clean_session ] else [] in
    let opt =
      Mqtt_packet.
        {
          ping_timeout;
          cxn_data = { clientid = id; credentials; will; flags; keep_alive };
        }
    in

    let rec try_connect hosts =
      match hosts with
      | [] ->
        Lwt.fail
          (Connection_error "Could not connect to any of the provided hosts")
      | host :: hosts -> (
        try%lwt
          let%lwt () =
            Log.info (fun log -> log "Connecting... host=%s port=%d" host port)
          in
          let%lwt connection =
            match tls_ca with
            | Some ca_file -> open_tls_connection ~ca_file host port
            | None -> open_tcp_connection host port
          in
          let%lwt () = Log.info (fun log -> log "Connected.") in
          Lwt.return connection
        with _ ->
          let%lwt () =
            Log.debug (fun log -> log "Could not connect, trying next...")
          in
          try_connect hosts)
    in

    try_connect hosts >>= fun ((ic, oc) as connection) ->
    let%lwt () = Log.debug (fun log -> log "Opened connection.") in

    (* Send the CONNECT packet to the server. *)
    let connect_packet =
      Mqtt_packet.Encoder.connect ?credentials:opt.cxn_data.credentials
        ?will:opt.cxn_data.will ~flags:opt.cxn_data.flags
        ~keep_alive:opt.cxn_data.keep_alive opt.cxn_data.clientid
    in
    Lwt_io.write oc connect_packet >>= fun () ->
    let inflight = Hashtbl.create 100 in

    match%lwt read_packet ic with
    | _, Connack { connection_status = Accepted; session_present } ->
      let%lwt () =
        Log.info (fun log ->
            log "Connected to borker; session_present=%b" session_present)
      in

      let pinger = Lwt.return_unit in
      let reader = Lwt.return_unit in

      let reset_ping_timer = Lwt_mvar.create_empty () in

      let client =
        {
          cxn = connection;
          inflight;
          reader;
          pinger;
          reset_ping_timer;
          should_stop_reader = Lwt_condition.create ();
          on_message;
          on_disconnect;
          on_error;
        }
      in

      let pinger =
        wrap_catch client
        @@ ping_timer client ~ping_timeout:opt.ping_timeout
             ~keep_alive:opt.cxn_data.keep_alive
      in
      let reader = wrap_catch client @@ read_packets client in

      Lwt.async (fun () -> pinger <&> reader >>= fun () -> shutdown client);

      client.pinger <- pinger;
      client.reader <- reader;

      Lwt.return client
    | _, Connack pkt ->
      Lwt.fail
        (Connection_error
           (Mqtt_packet.connection_status_to_string pkt.connection_status))
    | _ ->
      Lwt.fail (Connection_error "Invalid response from broker on connection")


  let publish ?(dup = false) ?(qos = Atleast_once) ?(retain = false) ~topic
      payload client =
    let _, oc = client.cxn in
    let id = Mqtt_packet.gen_id () in
    let cond = Lwt_condition.create () in
    let expected_ack_pkt = Mqtt_packet.puback id in
    Hashtbl.add client.inflight id (cond, expected_ack_pkt);
    let pkt_data =
      Mqtt_packet.Encoder.publish ~dup ~qos ~retain ~id ~topic payload
    in
    Lwt_io.write oc pkt_data >>= fun () -> Lwt_condition.wait cond


  let subscribe ?(dup = false) ?(qos = Atleast_once) ?(retain = false) topics
      client =
    let _, oc = client.cxn in
    let pkt_id = Mqtt_packet.gen_id () in
    let subscribe_packet =
      Mqtt_packet.Encoder.subscribe ~dup ~qos ~retain ~id:pkt_id topics
    in
    let qos_list = List.map (fun (_, q) -> q) topics in
    let cond = Lwt_condition.create () in
    Hashtbl.add client.inflight pkt_id (cond, Suback (pkt_id, qos_list));
    wrap_catch client (fun () ->
        let%lwt () = Lwt_io.write oc subscribe_packet in
        let%lwt () = Lwt_condition.wait cond in
        let topics = List.map fst topics in
        Log.info (fun log ->
            log "Subscribed to %a." Fmt.Dump.(list string) topics))
end
include Client
