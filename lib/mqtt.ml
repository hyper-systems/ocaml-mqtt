open Lwt

let (<&>) = Lwt.(<&>)
let fmt = Format.asprintf

module BE = EndianBytes.BigEndian


let encode_length len =
  let rec loop ll digits =
    if ll <= 0 then digits
    else
      let incr = Int32.logor (Int32.of_int 0x80) in
      let shft = Int32.logor (Int32.shift_left digits 8) in
      let getdig x dig = if x > 0 then incr dig else dig in
      let quotient = ll / 128 in
      let digit = getdig quotient (Int32.of_int (ll mod 128)) in
      let digits = shft digit in
      loop quotient digits in
  loop len 0l


let decode_length inch =
  let rec loop value mult =
    Lwt_io.read_char inch >>= fun ch ->
    let ch = Char.code ch in
    let digit = ch land 127 in
    let value = value + digit * mult in
    let mult = mult * 128 in
    if ch land 128 = 0 then Lwt.return value
    else loop value mult in
  loop 0 1


type t = (Lwt_io.input_channel * Lwt_io.output_channel)

type messages = Connect_pkt | Connack_pkt |
                Publish_pkt | Puback_pkt | Pubrec_pkt | Pubrel_pkt |
                Pubcomp_pkt | Subscribe_pkt | Suback_pkt |
                Unsubscribe_pkt | Unsuback_pkt | Pingreq_pkt |
                Pingresp_pkt | Disconnect_pkt

type qos = Atmost_once | Atleast_once | Exactly_once

type cxn_flags = Will_retain | Will_qos of qos | Clean_session

type cxn_userpass = Username of string | UserPass of (string * string)

type cxn_data = {
  clientid: string;
  userpass: cxn_userpass option;
  will: (string * string) option;
  flags: cxn_flags list;
  keep_alive: int;
}

type client_options = {
  ping_timeout: float;
  cxn_data: cxn_data;
}

type connection_status =
  | Accepted
  | Unacceptable_protocol_version
  | Identifier_rejected
  | Server_unavailable
  | Bad_username_or_password
  | Not_authorized


let connection_status_to_string = function
  | Accepted                      -> "Accepted"
  | Unacceptable_protocol_version -> "Unacceptable_protocol_version"
  | Identifier_rejected           -> "Identifier_rejected"
  | Server_unavailable            -> "Server_unavailable"
  | Bad_username_or_password      -> "Bad_username_or_password"
  | Not_authorized                -> "Not_authorized"


let connection_status_to_int = function
  | Accepted                      -> 0
  | Unacceptable_protocol_version -> 1
  | Identifier_rejected           -> 2
  | Server_unavailable            -> 3
  | Bad_username_or_password      -> 4
  | Not_authorized                -> 5


let connection_status_of_int = function
  | 0 -> Accepted
  | 1 -> Unacceptable_protocol_version
  | 2 -> Identifier_rejected
  | 3 -> Server_unavailable
  | 4 -> Bad_username_or_password
  | 5 -> Not_authorized
  | _ -> raise (Invalid_argument "Invalid connection status code")


type packet =
  | Connect of cxn_data
  | Connack of { session_present : bool; connection_status : connection_status }
  | Subscribe of (int * (string * qos) list)
  | Suback of (int * qos list)
  | Unsubscribe of (int * string list)
  | Unsuback of int
  | Publish of (int option * string * string)
  | Puback of int
  | Pubrec of int
  | Pubrel of int
  | Pubcomp of int
  | Pingreq
  | Pingresp
  | Disconnect


type pkt_opt = bool * qos * bool


let msgid = ref 0

let gen_id () =
  let () = incr msgid in
  if !msgid >= 0xFFFF then msgid := 1;
  !msgid

let int16be n =
  let s = Bytes.create 2 in
  BE.set_int16 s 0 n;
  s

let int8be n =
  let s = Bytes.create 1 in
  BE.set_int8 s 0 n;
  s

let trunc str =
  (* truncate leading zeroes *)
  let len = String.length str in
  let rec loop count =
    if count >= len || str.[count] <> '\000' then count
    else loop (count + 1) in
  let leading = loop 0 in
  if leading = len then "\000"
  else String.sub str leading (len - leading)

let addlen s =
  let len = String.length s in
  if len > 0xFFFF then raise (Invalid_argument "string too long");
  Bytes.to_string (int16be len) ^ s

let opt_with s n = function
  | Some a -> s a
  | None -> n

let bits_of_message = function
  | Connect_pkt -> 1
  | Connack_pkt -> 2
  | Publish_pkt -> 3
  | Puback_pkt  -> 4
  | Pubrec_pkt  -> 5
  | Pubrel_pkt  -> 6
  | Pubcomp_pkt -> 7
  | Subscribe_pkt -> 8
  | Suback_pkt  -> 9
  | Unsubscribe_pkt -> 10
  | Unsuback_pkt -> 11
  | Pingreq_pkt -> 12
  | Pingresp_pkt -> 13
  | Disconnect_pkt -> 14

let message_of_bits = function
  | 1 -> Connect_pkt
  | 2 -> Connack_pkt
  | 3 -> Publish_pkt
  | 4 -> Puback_pkt
  | 5 -> Pubrec_pkt
  | 6 -> Pubrel_pkt
  | 7 -> Pubcomp_pkt
  | 8 -> Subscribe_pkt
  | 9 -> Suback_pkt
  | 10 -> Unsubscribe_pkt
  | 11 -> Unsuback_pkt
  | 12 -> Pingreq_pkt
  | 13 -> Pingresp_pkt
  | 14 -> Disconnect_pkt
  | _ -> raise (Invalid_argument "invalid bits in message")

let bits_of_qos = function
  | Atmost_once -> 0
  | Atleast_once -> 1
  | Exactly_once -> 2

let qos_of_bits = function
  | 0 -> Atmost_once
  | 1 -> Atleast_once
  | 2 -> Exactly_once
  | _ -> raise (Invalid_argument "invalid qos number")

let bit_of_bool = function
  | true -> 1
  | false -> 0

let bool_of_bit = function
  | 1 -> true
  | 0 -> false
  | n -> raise (Invalid_argument ("expected zero or one, but got " ^ string_of_int n))


let fixed_header'new typ ~dup ~qos ~retain body_len =
  let msgid = (bits_of_message typ) lsl 4 in
  let dup = (bit_of_bool dup) lsl 3 in
  let qos = (bits_of_qos qos) lsl 1 in
  let retain = bit_of_bool retain in
  let hdr = Bytes.create 1 in
  let len = Bytes.create 4 in
  BE.set_int8 hdr 0 (msgid + dup + qos + retain);
  BE.set_int32 len 0 (encode_length body_len);
  let len = trunc (Bytes.to_string len) in
  Bytes.to_string hdr ^ len

let fixed_header typ (parms:pkt_opt) body_len =
  let (dup, qos, retain) = parms in
  let msgid = (bits_of_message typ) lsl 4 in
  let dup = (bit_of_bool dup) lsl 3 in
  let qos = (bits_of_qos qos) lsl 1 in
  let retain = bit_of_bool retain in
  let hdr = Bytes.create 1 in
  let len = Bytes.create 4 in
  BE.set_int8 hdr 0 (msgid + dup + qos + retain);
  BE.set_int32 len 0 (encode_length body_len);
  let len = trunc (Bytes.to_string len) in
  Bytes.to_string hdr ^ len

let pubpkt ?(opt = (false, Atmost_once, false)) typ id =
  let hdr = fixed_header typ opt 2 in
  let msgid = int16be id |> Bytes.to_string in
  let buf = Buffer.create 4 in
  Buffer.add_string buf hdr;
  Buffer.add_string buf msgid;
  Buffer.contents buf

let pubrec = pubpkt Pubrec_pkt

let pubrel ?opt = pubpkt ?opt Pubrel_pkt

let pubcomp = pubpkt Pubcomp_pkt

let suback ?(opt = (false, Atmost_once, false)) id qoses =
  let paylen = (List.length qoses) + 2 in
  let buf = Buffer.create (paylen + 5) in
  let msgid = int16be id |> Bytes.to_string in
  let q2i q = bits_of_qos q |> int8be |> Bytes.to_string in
  let blit q = Buffer.add_string buf (q2i q) in
  let hdr = fixed_header Suback_pkt opt paylen in
  Buffer.add_string buf hdr;
  Buffer.add_string buf msgid;
  List.iter blit qoses;
  Buffer.contents buf

let unsubscribe ?(opt = (false, Atleast_once, false)) ?(id = gen_id ()) topics =
  let accum acc i = acc + 2 + String.length i in
  let tl = List.fold_left accum 2 topics in (* +2 for msgid *)
  let buf = Buffer.create (tl + 5) in (* ~5 for fixed header *)
  let addtopic t = addlen t |> Buffer.add_string buf in
  let msgid = int16be id |> Bytes.to_string in
  let hdr = fixed_header Unsubscribe_pkt opt tl in
  Buffer.add_string buf hdr;
  Buffer.add_string buf msgid;
  List.iter addtopic topics;
  Buffer.contents buf

let unsuback id =
  let msgid = int16be id |> Bytes.to_string in
  let opt = (false, Atmost_once, false) in
  let hdr = fixed_header Unsuback_pkt opt 2 in
  hdr ^ msgid

let simple_pkt typ = fixed_header typ (false, Atmost_once, false) 0

let pingreq () = simple_pkt Pingreq_pkt

let pingresp () = simple_pkt Pingresp_pkt



module Packet = struct
  type t = packet

  let puback id = Puback id

  module Encoder = struct
    let puback = pubpkt Puback_pkt

    let disconnect () = simple_pkt Disconnect_pkt

    let subscribe ?(opt = (false, Atleast_once, false)) ?(id = gen_id ()) topics =
      let accum acc (i, _) = acc + 3 + String.length i in
      let tl = List.fold_left accum 0 topics in
      let tl = tl + 2 in (* add msgid to total len *)
      let buf = Buffer.create (tl + 5) in (* ~5 for fixed header *)
      let addtopic (t, q) =
        Buffer.add_string buf (addlen t);
        Buffer.add_string buf (Bytes.to_string @@ int8be (bits_of_qos q)) in
      let msgid = int16be id |> Bytes.to_string in
      let hdr = fixed_header Subscribe_pkt opt tl in
      Buffer.add_string buf hdr;
      Buffer.add_string buf msgid;
      List.iter addtopic topics;
      Buffer.contents buf


    let publish ?(opt = (false, Atmost_once, false)) ?(id = -1) topic payload =
      let (_, qos, _) = opt in
      let msgid =
        if qos = Atleast_once || qos = Exactly_once then
          let mid = if id = -1 then gen_id ()
            else id in int16be mid |> Bytes.to_string
        else "" in
      let topic = addlen topic in
      let sl = String.length in
      let tl = sl topic + sl payload + sl msgid in
      let buf = Buffer.create (tl + 5) in
      let hdr = fixed_header Publish_pkt opt tl in
      Buffer.add_string buf hdr;
      Buffer.add_string buf topic;
      Buffer.add_string buf msgid;
      Buffer.add_string buf payload;
      Buffer.contents buf


    let publish'new ~dup ~qos ~retain ~id ~topic payload =
      let id_data =
        if qos = Atleast_once || qos = Exactly_once then
          Bytes.to_string (int16be id)
        else ""
      in
      let topic = addlen topic in
      let sl = String.length in
      let tl = sl topic + sl payload + sl id_data in
      let buf = Buffer.create (tl + 5) in
      let hdr = fixed_header'new Publish_pkt ~dup ~qos ~retain tl in
      Buffer.add_string buf hdr;
      Buffer.add_string buf topic;
      Buffer.add_string buf id_data;
      Buffer.add_string buf payload;
      Buffer.contents buf


    let connect_payload ?userpass ?will ?(flags = []) ?(keep_alive = 10) id =
      let name = addlen "MQIsdp" in
      let version = "\003" in
      if keep_alive > 0xFFFF then raise (Invalid_argument "keep_alive too large");
      let addhdr2 flag term (flags, hdr) = match term with
        | None -> flags, hdr
        | Some (a, b) -> (flags lor flag),
                         (hdr ^ (addlen a) ^ (addlen b)) in
      let adduserpass term (flags, hdr) = match term with
        | None -> flags, hdr
        | Some (Username s) -> (flags lor 0x80), (hdr ^ addlen s)
        | Some (UserPass up) ->
          addhdr2 0xC0 (Some up) (flags, hdr) in
      let flag_nbr = function
        | Clean_session -> 0x02
        | Will_qos qos -> (bits_of_qos qos) lsl 3
        | Will_retain -> 0x20 in
      let accum a acc = acc  lor (flag_nbr a) in
      let flags, pay =
        ((List.fold_right accum flags 0), (addlen id))
        |> addhdr2 0x04 will |> adduserpass userpass in
      let tbuf = int16be keep_alive in
      let fbuf = Bytes.create 1 in
      BE.set_int8 fbuf 0 flags;
      let accum acc a = acc + (String.length a) in
      let fields = [name; version; Bytes.to_string fbuf; Bytes.to_string tbuf; pay] in
      let lens = List.fold_left accum 0 fields in
      let buf = Buffer.create lens in
      List.iter (Buffer.add_string buf) fields;
      Buffer.contents buf

    let connect ?userpass ?will ?flags ?keep_alive ?(opt = (false, Atmost_once, false)) id =
      let cxn_pay = connect_payload ?userpass ?will ?flags ?keep_alive id in
      let hdr = fixed_header Connect_pkt opt (String.length cxn_pay) in
      hdr ^ cxn_pay

    let connect_data d =
      let clientid = d.clientid in
      let userpass = d.userpass in
      let will = d.will in
      let flags = d.flags in
      let keep_alive = d.keep_alive in
      connect_payload ?userpass ?will ~flags ~keep_alive clientid

    let connack ?(opt = (false, Atmost_once, false)) ~session_present status =
      let fixed_header = fixed_header Connack_pkt opt 2 in
      let flags = Bytes.to_string (int8be (bit_of_bool session_present)) in
      let connection_status = Bytes.to_string (int8be (connection_status_to_int status)) in
      let variable_header = flags ^ connection_status in
      fixed_header ^ variable_header
  end
end



let decode_connect rb =
  let lead = Read_buffer.read rb 9 in
  if "\000\006MQIsdp\003" <> lead then
    raise (Invalid_argument "invalid MQIsdp or version");
  let hdr = Read_buffer.read_uint8 rb in
  let keep_alive = Read_buffer.read_uint16 rb in
  let has_username = 0 <> (hdr land 0x80) in
  let has_password = 0 <> (hdr land 0xC0) in
  let will_flag = bool_of_bit ((hdr land 0x04) lsr 2) in
  let will_retain = will_flag && 0 <> (hdr land 0x20) in
  let will_qos = if will_flag then
      Some (qos_of_bits ((hdr land 0x18) lsr 3)) else None in
  let clean_session = bool_of_bit ((hdr land 0x02) lsr 1) in
  let rs = Read_buffer.read_string in
  let clientid = rs rb in
  let will = if will_flag then
      let t = rs rb in
      let m = rs rb in
      Some (t, m)
    else None in
  let userpass = if has_password then
      let u = rs rb in
      let p = rs rb in
      Some (UserPass (u, p))
    else if has_username then Some (Username (rs rb))
    else None in
  let flags = if clean_session then [ Clean_session ] else [] in
  let flags = opt_with (fun qos -> (Will_qos qos) :: flags ) flags will_qos in
  let flags = if will_retain then Will_retain :: flags else flags in
  Connect {clientid; userpass; will; flags; keep_alive;}

let decode_connack rb =
  let flags = Read_buffer.read_uint8 rb in
  let session_present = bool_of_bit flags in
  let connection_status = connection_status_of_int (Read_buffer.read_uint8 rb) in
  Connack { session_present; connection_status }

let decode_publish (_, qos, _) rb =
  let topic = Read_buffer.read_string rb in
  let msgid = if qos = Atleast_once || qos = Exactly_once then
      Some (Read_buffer.read_uint16 rb)
    else None in
  let payload = Read_buffer.len rb |> Read_buffer.read rb in
  Publish (msgid, topic, payload)

let decode_puback rb = Puback (Read_buffer.read_uint16 rb)

let decode_pubrec rb = Pubrec (Read_buffer.read_uint16 rb)

let decode_pubrel rb = Pubrel (Read_buffer.read_uint16 rb)

let decode_pubcomp rb = Pubcomp (Read_buffer.read_uint16 rb)

let decode_subscribe rb =
  let id = Read_buffer.read_uint16 rb in
  let get_topic rb =
    let topic = Read_buffer.read_string rb in
    let qos = Read_buffer.read_uint8 rb |> qos_of_bits in
    (topic, qos) in
  let topics = Read_buffer.read_all rb get_topic in
  Subscribe (id, topics)

let decode_suback rb =
  let id = Read_buffer.read_uint16 rb in
  let get_qos rb = Read_buffer.read_uint8 rb |> qos_of_bits in
  let qoses = Read_buffer.read_all rb get_qos in
  Suback (id, List.rev qoses)

let decode_unsub rb =
  let id = Read_buffer.read_uint16 rb in
  let topics = Read_buffer.read_all rb Read_buffer.read_string in
  Unsubscribe (id, topics)

let decode_unsuback rb = Unsuback (Read_buffer.read_uint16 rb)

let decode_pingreq _rb = Pingreq

let decode_pingresp _rb = Pingresp

let decode_disconnect _rb = Disconnect

let decode_packet opts = function
  | Connect_pkt -> decode_connect
  | Connack_pkt -> decode_connack
  | Publish_pkt -> decode_publish opts
  | Puback_pkt -> decode_puback
  | Pubrec_pkt -> decode_pubrec
  | Pubrel_pkt -> decode_pubrel
  | Pubcomp_pkt -> decode_pubcomp
  | Subscribe_pkt -> decode_subscribe
  | Suback_pkt -> decode_suback
  | Unsubscribe_pkt -> decode_unsub
  | Unsuback_pkt -> decode_unsuback
  | Pingreq_pkt -> decode_pingreq
  | Pingresp_pkt -> decode_pingresp
  | Disconnect_pkt -> decode_disconnect

let decode_fixed_header byte : messages * pkt_opt =
  let typ = (byte land 0xF0) lsr 4 in
  let dup = (byte land 0x08) lsr 3 in
  let qos = (byte land 0x06) lsr 1 in
  let retain = byte land 0x01 in
  let typ = message_of_bits typ in
  let dup = bool_of_bit dup in
  let qos = qos_of_bits qos in
  let retain = bool_of_bit retain in
  (typ, (dup, qos, retain))

let read_packet inch =
  Lwt_io.read_char inch >>= fun header_byte ->
  let (msgid, opts) = decode_fixed_header (Char.code header_byte) in
  decode_length inch >>= fun count ->
  let data = Bytes.create count in
  let rd = try Lwt_io.read_into_exactly inch data 0 count
    with End_of_file -> Lwt.fail (Failure "could not read bytes") in
  rd >>= fun () ->
  let pkt = Read_buffer.make (data |> Bytes.to_string) |> decode_packet opts msgid in
  Lwt.return (opts, pkt)



module Client = struct
  let src = Logs.Src.create "mqtt.client" ~doc:"MQTT Client module"
  module Log = (val Logs_lwt.src_log src : Logs_lwt.LOG)

  type client = {
    cxn : t;
    stream: (string * string) Lwt_stream.t;
    push : ((string * string) option -> unit);
    inflight : (int, (unit Lwt_condition.t * packet)) Hashtbl.t;
    mutable reader : unit Lwt.t;
    mutable pinger : unit Lwt.t;
    on_error : (client -> exn -> unit Lwt.t);
    reset_ping_timer : unit Lwt_mvar.t;
    should_stop_reader : unit Lwt_condition.t;
  }

  let default_error_fn _client exn =
    Lwt_io.printlf "mqtt error: %s\n%s" (Printexc.to_string exn) (Printexc.get_backtrace ())

  let read_packets client () =
    let ((in_chan, out_chan)) = client.cxn in

    let ack_inflight id pkt =
      try
        let (cond, expected_ack_pkt) = Hashtbl.find client.inflight id in
        if pkt = expected_ack_pkt then begin
          Hashtbl.remove client.inflight id;
          Lwt_condition.signal cond ();
          Lwt.return_unit
        end else
          Lwt.fail (Failure ("unexpected packet in ack"))
      with Not_found ->
        Lwt.fail (Failure (fmt "ack for id=%d not found" id))
    in

    let push topic payload =
      Lwt.return (client.push (Some (topic, payload)))
    in

    let _push_id id pkt_data topic payload =
      ack_inflight id pkt_data >>= fun () ->
      push topic payload
    in

    let rec loop () =
      read_packet in_chan >>= fun ((_dup, qos, _retain), packet) -> begin
        match packet with
        (* Publish with QoS 0: push *)
        | Publish (None, topic, payload) when qos = Atmost_once ->
          push topic payload

        (* Publish with QoS 0 and packet identifier: error *)
        | Publish (Some _id, _topic, _payload) when qos = Atmost_once ->
          Lwt.fail (Failure "protocol violation: publish packet with qos 0 must not have id")

        (* Publish with QoS 1 *)
        | Publish (Some id, topic, payload) when qos = Atleast_once ->
          (* - Push the message to the consumer queue.
             - Send back the PUBACK packet. *)
          push topic payload >>= fun () ->
          let puback = Packet.Encoder.puback id in
          Lwt_io.write out_chan puback >>= fun () ->
          Lwt.return_unit

        | Publish (None, _topic, _payload) when qos = Atleast_once ->
          Lwt.fail (Failure "protocol violation: publish packet with qos > 0 must have id")

        | Publish _ ->
          Lwt.fail (Failure "not supported publish packet (probably qos 2)")

        | Suback (id, _)
        | Unsuback id
        | Puback id
        | Pubrec id
        | Pubrel id
        | Pubcomp id ->
          ack_inflight id packet

        | Pingresp ->
          let%lwt () = Log.debug (fun log ->
              if not (Lwt_mvar.is_empty client.reset_ping_timer) then
                log "Reset ping timer mvar is not empty, will block."
              else Lwt.return_unit) in

          Lwt_mvar.put client.reset_ping_timer ()

        | _ -> Lwt.fail (Failure "unknown packet from server")
      end >>= fun () -> loop ()
    in
    let%lwt () = Log.debug (fun log -> log "Starting reader loop...") in
    Lwt.pick [
      (Lwt_condition.wait client.should_stop_reader >>= fun () -> Log.info (fun log -> log "Stopping reader loop..."));
      loop ()
    ]


  let wrap_catch client f = Lwt.catch f (client.on_error client)

  exception Ping_timeout


  let disconnect client =
    let%lwt () = Log.info (fun log -> log "Disconnecting client...") in
    let (_, oc) = client.cxn in

    (* Notify the subscriber about termination. *)
    client.push None;

    (* Stop reading packets. *)
    Lwt_condition.signal client.should_stop_reader ();

    (* Send the disconnect packet to server. *)
    let%lwt () = Lwt_io.write oc (Packet.Encoder.disconnect ()) in
    Log.info (fun log -> log "Did disconnect client...")


  let shutdown client =
    let%lwt () = Log.debug (fun log -> log "Shutting down the connection...") in
    let (ic, oc) = client.cxn in
    let%lwt () = Lwt_io.flush oc in
    let%lwt () = Lwt_io.close ic in
    let%lwt () = Lwt_io.close oc in
    Log.debug (fun log -> log "Did shut down the connection.")


  let ping_timer client ?(ping_timeout = 5.0) ~keep_alive () =
    let%lwt () = Log.debug (fun log -> log "Starting ping timer...") in
    let (_, output) = client.cxn in
    let keep_alive = 0.9 *. float_of_int keep_alive in (* 10% leeway *)
    let rec loop () =
      (* Wait for keep alive interval. *)
      let%lwt () = Log.debug (fun log -> log "Waiting for keep_alive interval...") in
      let%lwt () = Lwt_unix.sleep keep_alive in

      (* Send PINGREQ. *)
      let pingreq_packet = pingreq () in
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
      Lwt.catch (fun () -> Lwt.choose [timeout; reset])
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
      Lwt.fail (Connection_error ("Error: mqtt: could not get address info for " ^ host))


  let connect
      ?(id = "OCamlMQTT") ?tls_ca ?credentials ?will ?(clean_session=true)
      ?(keep_alive = 10) ?(ping_timeout = 5.0) ?(on_error = default_error_fn) ?(port = 1883) hosts =

    let flags = if clean_session then [Clean_session] else [] in
    let opt = { ping_timeout; cxn_data = { clientid = id; userpass = credentials; will; flags; keep_alive } } in


    let rec try_connect hosts =
      match hosts with
      | [] -> Lwt.fail (Connection_error "Could not connect to provided hosts")
      | host :: hosts ->
        try%lwt
          let connection =
            match tls_ca with
            | Some ca_file -> open_tls_connection ~ca_file host port
            | None -> open_tcp_connection host port in
          let%lwt () = Log.info (fun log -> log "Connected. host=%s port=%d" host port) in
          connection
        with _ ->
          let%lwt () = Log.debug (fun log -> log "Could not connect to host=%s port=%d, trying next..." host port) in
          try_connect hosts
    in

    try_connect hosts >>= fun ((ic, oc) as connection) ->
    let%lwt () = Log.debug (fun log -> log "Opened connection.") in

    (* Send the CONNECT packet to the server. *)
    let connect_packet = Packet.Encoder.connect
        ?userpass:opt.cxn_data.userpass
        ?will:opt.cxn_data.will
        ~flags:opt.cxn_data.flags
        ~keep_alive:opt.cxn_data.keep_alive
        opt.cxn_data.clientid
    in
    Lwt_io.write oc connect_packet >>= fun () ->

    let stream, push = Lwt_stream.create () in
    let inflight = Hashtbl.create 100 in

    match%lwt read_packet ic with
    | (_, Connack { connection_status = Accepted; session_present }) ->
      let%lwt () = Log.info (fun log -> log "Connected to borker... session_present=%b" session_present) in

      let pinger = Lwt.return_unit in
      let reader = Lwt.return_unit in

      let reset_ping_timer = Lwt_mvar.create_empty () in

      let client = {
        cxn = connection;
        stream;
        push;
        inflight;
        reader;
        pinger;
        reset_ping_timer;
        should_stop_reader = Lwt_condition.create ();
        on_error;
      } in

      let pinger = wrap_catch client @@ ping_timer client ~ping_timeout:opt.ping_timeout ~keep_alive:opt.cxn_data.keep_alive in
      let reader = wrap_catch client @@ read_packets client in

      Lwt.async (fun () -> pinger <&> reader >>= fun () -> shutdown client);

      client.pinger <- pinger;
      client.reader <- reader;

      Lwt.return client

    | (_, Connack pkt) ->
      Lwt.fail (Connection_error (connection_status_to_string pkt.connection_status))

    | _ -> Lwt.fail (Connection_error ("Invalid response from broker on connection"))


  let connection c = c.cxn


  let publish ?(dup=false) ~qos ?(retain=false) client topic payload =
    let (_, oc) = client.cxn in
    let id = !msgid in
    let cond = Lwt_condition.create () in
    let expected_ack_pkt = Packet.puback id in
    Hashtbl.add client.inflight id (cond, expected_ack_pkt);
    let pkt_data =
      Packet.Encoder.publish'new ~dup ~qos ~retain ~id ~topic payload in
    Lwt_io.write oc pkt_data >>= fun () ->
    Lwt_condition.wait cond


  let subscribe ?opt ?id client topics =
    let (_, oc) = client.cxn in
    let subscribe_packet = Packet.Encoder.subscribe ?opt ?id topics in
    let qos_list = List.map (fun (_, q) -> q) topics in
    let pkt_id = !msgid in
    let cond = Lwt_condition.create () in
    Hashtbl.add client.inflight pkt_id (cond, (Suback (pkt_id, qos_list)));
    wrap_catch client (fun () ->
        Lwt_io.write oc subscribe_packet >>= fun () ->
        Lwt_condition.wait cond >>= fun _ ->
        Lwt.return_unit)

  let messages client = client.stream
end


module Server = struct
  type t = Lwt_io.server

  let () =
    let open Sys in
    set_signal sigpipe Signal_ignore

  let cxns = ref []

  let handle_sub outch s =
    cxns := outch :: !cxns;
    let (msgid, list) = s in
    let qoses = List.map (fun (_, q) -> q) list in
    suback msgid qoses |> Lwt_io.write outch

  let handle_pub p =
    let (_, topic, payload) = p in
    let s = Packet.Encoder.publish topic payload in
    let write ch = Lwt_io.write ch s in
    Lwt_list.iter_p write !cxns

  let srv_cxn _client_address cxn =
    let (inch, outch) = cxn in
    Lwt.catch (fun () ->
        read_packet inch >>= (function
            | (_, Connect _) ->
              let connack = Packet.Encoder.connack ~session_present:false Accepted in
              Lwt_io.write outch connack
            | _ -> Lwt.fail (Failure "Mqtt Server: Expected connect")) >>= fun () ->
        let rec loop g =
          read_packet inch >>= (function
              | (_, Publish s) -> handle_pub s
              | (_, Subscribe s) -> handle_sub outch s
              | (_, Pingreq) -> pingresp () |> Lwt_io.write outch
              | (_, Disconnect) -> Lwt_io.printl "Disconnected client"
              | _ -> Lwt.fail (Failure "Mqtt Server: Unknown paqet")) >>= fun () ->
          loop g in
        loop ())
      (function Unix.Unix_error (Unix.EPIPE, _, _)
              | Unix.Unix_error (Unix.ECONNRESET, _, _)
              | Unix.Unix_error (Unix.ENOTCONN, _, _)
              | End_of_file ->
        Printf.printf "Cleaning up client\n%!";
        cxns := List.filter (fun ch -> ch != outch) !cxns;
        Lwt_io.close inch <&> Lwt_io.close outch >>= fun () ->
        Lwt.return_unit
              | exn -> Lwt_io.printlf "SRVERR: %s" (Printexc.to_string exn))

  let addr host port =
    Lwt_unix.gethostbyname host >>= fun hostent ->
    let inet_addr = hostent.Unix.h_addr_list.(0) in
    Unix.ADDR_INET (inet_addr, port) |> Lwt.return

  let listen ?(host = "localhost") ?(port = 1883) () =
    addr host port >>= fun a ->
    Lwt_io.establish_server_with_client_address ~backlog:1000 a srv_cxn
end

