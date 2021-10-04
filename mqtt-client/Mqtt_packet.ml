module BE = EndianBytes.BigEndian

open Mqtt_core

let msgid = ref 1

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


type messages =
  | Connect_pkt
  | Connack_pkt
  | Publish_pkt
  | Puback_pkt
  | Pubrec_pkt
  | Pubrel_pkt
  | Pubcomp_pkt
  | Subscribe_pkt
  | Suback_pkt
  | Unsubscribe_pkt
  | Unsuback_pkt
  | Pingreq_pkt
  | Pingresp_pkt
  | Disconnect_pkt




type cxn_flags =
  | Will_retain
  | Will_qos of qos
  | Clean_session


type cxn_data = {
  clientid: string;
  credentials: credentials option;
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


type t =
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


type options = bool * qos * bool

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
  | b -> raise (Invalid_argument ("invalid qos number: " ^ string_of_int b))

let bit_of_bool = function
  | true -> 1
  | false -> 0

let bool_of_bit = function
  | 1 -> true
  | 0 -> false
  | n -> raise (Invalid_argument ("expected zero or one, but got " ^ string_of_int n))




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



let puback id = Puback id


module Encoder = struct


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


let fixed_header typ ~dup ~qos ~retain body_len =
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




let unsubscribe ~dup ~qos ~retain ?(id = gen_id ()) topics =
  let accum acc i = acc + 2 + String.length i in
  let tl = List.fold_left accum 2 topics in (* +2 for msgid *)
  let buf = Buffer.create (tl + 5) in (* ~5 for fixed header *)
  let addtopic t = addlen t |> Buffer.add_string buf in
  let msgid = int16be id |> Bytes.to_string in
  let hdr = fixed_header Unsubscribe_pkt ~dup ~qos ~retain tl in
  Buffer.add_string buf hdr;
  Buffer.add_string buf msgid;
  List.iter addtopic topics;
  Buffer.contents buf

let unsuback id =
  let msgid = int16be id |> Bytes.to_string in
  let hdr = fixed_header Unsuback_pkt ~dup:false ~qos:Atmost_once ~retain:false 2 in
  hdr ^ msgid

let simple_pkt typ =
  fixed_header typ ~dup:false ~qos:Atmost_once ~retain:false 0

let pingreq () = simple_pkt Pingreq_pkt

let pingresp () = simple_pkt Pingresp_pkt

let pubpkt ?(dup=false) ?(qos=Atmost_once) ?(retain=false) typ id =
  let hdr = fixed_header typ ~dup ~qos ~retain 2 in
  let msgid = int16be id |> Bytes.to_string in
  let buf = Buffer.create 4 in
  Buffer.add_string buf hdr;
  Buffer.add_string buf msgid;
  Buffer.contents buf

let pubrec = pubpkt Pubrec_pkt

let pubrel ?dup ?qos ?retain =
  pubpkt ?dup ?qos ?retain Pubrel_pkt

let pubcomp = pubpkt Pubcomp_pkt

let suback ?(dup=false) ?(qos=Atmost_once) ?(retain=false) id qoses =
  let paylen = (List.length qoses) + 2 in
  let buf = Buffer.create (paylen + 5) in
  let msgid = int16be id |> Bytes.to_string in
  let q2i q = bits_of_qos q |> int8be |> Bytes.to_string in
  let blit q = Buffer.add_string buf (q2i q) in
  let hdr = fixed_header Suback_pkt ~dup ~qos ~retain paylen in
  Buffer.add_string buf hdr;
  Buffer.add_string buf msgid;
  List.iter blit qoses;
  Buffer.contents buf

  let puback = pubpkt Puback_pkt

  let disconnect () = simple_pkt Disconnect_pkt

  let subscribe ~dup ~qos ~retain ?(id = gen_id ()) topics =
    let accum acc (i, _) = acc + 3 + String.length i in
    let tl = List.fold_left accum 0 topics in
    let tl = tl + 2 in (* add msgid to total len *)
    let buf = Buffer.create (tl + 5) in (* ~5 for fixed header *)
    let addtopic (t, q) =
      Buffer.add_string buf (addlen t);
      Buffer.add_string buf (Bytes.to_string @@ int8be (bits_of_qos q)) in
    let msgid = int16be id |> Bytes.to_string in
    let hdr = fixed_header Subscribe_pkt ~dup ~qos ~retain tl in
    Buffer.add_string buf hdr;
    Buffer.add_string buf msgid;
    List.iter addtopic topics;
    Buffer.contents buf

  let publish ~dup ~qos ~retain ~id ~topic payload =
    let id_data =
      if qos = Atleast_once || qos = Exactly_once then
        Bytes.to_string (int16be id)
      else ""
    in
    let topic = addlen topic in
    let sl = String.length in
    let tl = sl topic + sl payload + sl id_data in
    let buf = Buffer.create (tl + 5) in
    let hdr = fixed_header Publish_pkt ~dup ~qos ~retain tl in
    Buffer.add_string buf hdr;
    Buffer.add_string buf topic;
    Buffer.add_string buf id_data;
    Buffer.add_string buf payload;
    Buffer.contents buf


  let connect_payload ?credentials ?will ?(flags = []) ?(keep_alive = 10) id =
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
      | Some (Credentials (u, p)) ->
        addhdr2 0xC0 (Some (u, p)) (flags, hdr) in
    let flag_nbr = function
      | Clean_session -> 0x02
      | Will_qos qos -> (bits_of_qos qos) lsl 3
      | Will_retain -> 0x20 in
    let accum a acc = acc  lor (flag_nbr a) in
    let flags, pay =
      ((List.fold_right accum flags 0), (addlen id))
      |> addhdr2 0x04 will |> adduserpass credentials in
    let tbuf = int16be keep_alive in
    let fbuf = Bytes.create 1 in
    BE.set_int8 fbuf 0 flags;
    let accum acc a = acc + (String.length a) in
    let fields = [name; version; Bytes.to_string fbuf; Bytes.to_string tbuf; pay] in
    let lens = List.fold_left accum 0 fields in
    let buf = Buffer.create lens in
    List.iter (Buffer.add_string buf) fields;
    Buffer.contents buf

  let connect ?credentials ?will ?flags ?keep_alive ?(dup=false) ?(qos=Atmost_once) ?(retain=false) id =
    let cxn_pay = connect_payload ?credentials ?will ?flags ?keep_alive id in
    let hdr = fixed_header Connect_pkt ~dup ~qos ~retain (String.length cxn_pay) in
    hdr ^ cxn_pay

  let connect_data d =
    let clientid = d.clientid in
    let credentials = d.credentials in
    let will = d.will in
    let flags = d.flags in
    let keep_alive = d.keep_alive in
    connect_payload ?credentials ?will ~flags ~keep_alive clientid

  let connack ?(dup=false) ?(qos=Atmost_once) ?(retain=false) ~session_present status =
    let fixed_header = fixed_header Connack_pkt ~dup ~qos ~retain 2 in
    let flags = Bytes.to_string (int8be (bit_of_bool session_present)) in
    let connection_status = Bytes.to_string (int8be (connection_status_to_int status)) in
    let variable_header = flags ^ connection_status in
    fixed_header ^ variable_header
end


module Decoder = struct





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
    let credentials = if has_password then
        let u = rs rb in
        let p = rs rb in
        Some (Credentials (u, p))
      else if has_username then Some (Username (rs rb))
      else None in
    let flags = if clean_session then [ Clean_session ] else [] in
    let flags = opt_with (fun qos -> (Will_qos qos) :: flags ) flags will_qos in
    let flags = if will_retain then Will_retain :: flags else flags in
    Connect {clientid; credentials; will; flags; keep_alive;}

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

  let decode_fixed_header byte : messages * options =
    let typ = (byte land 0xF0) lsr 4 in
    let dup = (byte land 0x08) lsr 3 in
    let qos = (byte land 0x06) lsr 1 in
    let retain = byte land 0x01 in
    let typ = message_of_bits typ in
    let dup = bool_of_bit dup in
    let qos = qos_of_bits qos in
    let retain = bool_of_bit retain in
    (typ, (dup, qos, retain))
end
