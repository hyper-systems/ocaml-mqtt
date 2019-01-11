open OUnit
open Lwt

let (<&>) = Lwt.(<&>)


let fmt = Format.asprintf

module Mqtt
(* : sig *)

(*     type t *)
(*     type 'a monad = 'a Lwt.t *)
(*     type qos = Atmost_once | Atleast_once | Exactly_once *)
(*     type cxn_flags = Will_retain | Will_qos of qos | Clean_session *)
(*     type cxn_userpass = Username of string | UserPass of (string * string) *)
(*     type cxn_data = { *)
(*         clientid: string; *)
(*         userpass: cxn_userpass option; *)
(*         will: (string * string) option; *)
(*         flags: cxn_flags list; *)
(*         timer: int; *)
(*     } *)
(*     type cxnack_flags = Cxnack_accepted | Cxnack_protocol | Cxnack_id | *)
(*                     Cxnack_unavail | Cxnack_userpass | Cxnack_auth *)
(*     type packet = Connect of cxn_data | Connack of cxnack_flags | *)
(*                     Subscribe of (int * (string * qos) list) | *)
(*                     Suback of (int * qos list) | *)
(*                     Unsubscribe of (int * string list) | *)
(*                     Unsuback of int | *)
(*                     Publish of (int option * string * string) | *)
(*                     Puback of int | Pubrec of int | Pubrel of int | *)
(*                     Pubcomp of int | Pingreq | Pingresp | Disconnect | *)
(*                     Asdf *)
(*     type pkt_opt = bool * qos * bool *)

(*     val connect : ?userpass:cxn_userpass -> ?will:(string * string) -> *)
(*                 ?flags:cxn_flags list -> ?timer:int -> ?opt:pkt_opt -> *)
(*                 string -> string *)

(*     val connack : ?opt:pkt_opt -> cxnack_flags -> string *)

(*     val publish : ?opt:pkt_opt -> ?id:int -> string -> string -> string *)

(*     val puback : int -> string *)

(*     val pubrec : int -> string *)

(*     val pubrel : ?opt:pkt_opt -> int -> string *)

(*     val pubcomp : int -> string *)

(*     val subscribe : ?opt:pkt_opt -> ?id:int -> (string * qos) list -> string *)

(*     val suback : ?opt:pkt_opt -> int -> qos list -> string *)

(*     val unsubscribe: ?opt:pkt_opt -> ?id:int -> string list -> string *)

(*     val unsuback : int -> string *)

(*     val pingreq : unit -> string *)

(*     val pingresp : unit -> string *)

(*     val disconnect : unit -> string *)

(*     val read_packet : t -> (pkt_opt * packet) monad *)

(*     val tests : OUnit.test list *)

(*     module MqttClient : sig *)
(*         type client *)

(*         val connection : client -> Lwt_io.input_channel * Lwt_io.output_channel *)

(*         val connect_options : ?clientid:string -> ?userpass:cxn_userpass -> ?will:(string * string) -> ?flags:cxn_flags list -> ?timer:int -> unit -> cxn_data *)

(*         val connect : ?opt:cxn_data -> ?error_fn:(client -> exn -> unit monad) -> ?port:int -> string -> client monad *)
(*         val publish : ?opt:pkt_opt -> ?id:int -> client -> string -> string -> unit monad *)

(*         val subscribe : ?opt:pkt_opt -> ?id:int -> client -> (string * qos) list -> unit monad *)

(*         val disconnect : client -> unit monad *)

(*         val sub_stream : client -> (string * string) Lwt_stream.t *)

(*     end *)

(*     module MqttServer : sig *)
(*         type t *)

(*         val listen : ?host:string -> ?port:int -> unit -> t monad *)

(*     end *)

(* end *)
= struct

module BE = EndianBytes.BigEndian

module ReadBuffer : sig

    type t
    (* val create : unit -> t *)
    val make : string -> t
    (* val add_string : t -> string -> unit *)
    val len : t -> int
    val read: t -> int -> string
    val read_string : t -> string
    val read_uint8 : t -> int
    val read_uint16 : t -> int
    val read_all : t -> (t -> 'a) -> 'a list
    val tests : OUnit.test list

end = struct

type t = {
    mutable pos: int;
    mutable buf: bytes;
}

let create () = {pos=0; buf= Bytes.of_string ""}

let add_string rb str =
    let str = Bytes.of_string str in
    let curlen = (Bytes.length rb.buf) - rb.pos in
    let strlen = Bytes.length str in
    let newlen = strlen + curlen in
    let newbuf = Bytes.create newlen in
    Bytes.blit rb.buf rb.pos newbuf 0 curlen;
    Bytes.blit str 0 newbuf curlen strlen;
    rb.pos <- 0;
    rb.buf <- newbuf

let make str =
    let rb = create () in
    add_string rb str;
    rb

let len rb = (Bytes.length rb.buf) - rb.pos

let read rb count =
    let len = (Bytes.length rb.buf) - rb.pos in
    if count < 0 || len < count then
        raise (Invalid_argument "buffer underflow");
    let ret = Bytes.sub rb.buf rb.pos count in
    rb.pos <- rb.pos + count;
    Bytes.to_string ret

let read_uint8 rb =
    let str = rb.buf in
    let slen = (Bytes.length str) - rb.pos in
    if slen < 1 then raise (Invalid_argument "string too short");
    let res = BE.get_uint8 str rb.pos in
    rb.pos <- rb.pos + 1;
    res

let read_uint16 rb =
    let str = rb.buf in
    let slen = (Bytes.length str) - rb.pos in
    if slen < 2 then raise (Invalid_argument "string too short");
    let res = BE.get_uint16 str rb.pos in
    rb.pos <- rb.pos + 2;
    res

let read_string rb = read_uint16 rb |> read rb

let read_all rb f =
    let rec loop res =
        if (len rb) <= 0 then res
        else loop (f rb :: res) in
    loop []

module ReadBufferTests : sig
    val tests : OUnit.test list
end = struct

let test_create _ =
    let rb = create () in
    assert_equal 0 rb.pos;
    assert_equal (Bytes.empty) rb.buf

let test_add _ =
    let rb = create () in
    add_string rb "asdf";
    assert_equal (Bytes.of_string "asdf") rb.buf;
    add_string rb "qwerty";
    assert_equal (Bytes.of_string "asdfqwerty") rb.buf;
    (* test appends via manually resetting pos *)
    rb.pos <- 4;
    add_string rb "poiuy";
    assert_equal (Bytes.of_string "qwertypoiuy") rb.buf;
    assert_equal 0 rb.pos

let test_make _ =
    let rb = make "asdf" in
    assert_equal 0 rb.pos;
    assert_equal (Bytes.of_string "asdf") rb.buf

let test_len _ =
    let rb = create () in
    assert_equal 0 (len rb);
    add_string rb "asdf";
    assert_equal 4 (len rb);
    let _ = read rb 2 in
    assert_equal 2 (len rb);
    let _ = read rb 2 in
    assert_equal 0 (len rb)

let test_read _ =
    let rb = create () in
    let exn = Invalid_argument "buffer underflow" in
    assert_raises exn (fun () -> read rb 1);
    let rb = make "asdf" in
    assert_raises exn (fun () -> read rb (-1));
    assert_equal "" (read rb 0);
    assert_equal "as" (read rb 2);
    assert_raises exn (fun () -> read rb 3);
    assert_equal 2 rb.pos;
    assert_equal "df" (read rb 2);
    assert_raises exn (fun () -> read rb 1);
    assert_equal 4 rb.pos

let test_uint8 _ =
    let printer = string_of_int in
    let rb = create () in
    let exn = Invalid_argument "string too short" in
    assert_raises exn (fun () -> read_uint8 rb);
    let rb = make "\001\002\255" in
    assert_equal 1 (read_uint8 rb);
    assert_equal 1 rb.pos;
    assert_equal 2 (read_uint8 rb);
    assert_equal 2 rb.pos;
    assert_equal ~printer 255 (read_uint8 rb)

let test_int16 _ =
    let printer = string_of_int in
    let rb = create () in
    let exn = Invalid_argument "string too short" in
    assert_raises exn (fun () -> read_uint16 rb);
    let rb = make "\001" in
    assert_raises exn (fun () -> read_uint16 rb);
    let rb = make "\001\002" in
    assert_equal 258 (read_uint16 rb);
    let rb = make "\255\255\128" in
    assert_equal ~printer 65535 (read_uint16 rb)

let test_readstr _ =
    let rb = create () in
    let exn1 = Invalid_argument "string too short" in
    let exn2 = Invalid_argument "buffer underflow" in
    assert_raises exn1 (fun () -> read_string rb);
    let rb = make "\000" in
    assert_raises exn1 (fun () -> read_string rb);
    let rb = make "\000\001" in
    assert_raises exn2 (fun () -> read_string rb);
    let rb = make "\000\004asdf\000\006qwerty" in
    assert_equal "asdf" (read_string rb);
    assert_equal 6 rb.pos;
    assert_equal "qwerty" (read_string rb);
    assert_equal 14 rb.pos

let test_readall _ =
    let rb = make "\001\002\003\004\005" in
    let res = read_all rb read_uint8 in
    assert_equal res [5;4;3;2;1];
    assert_equal 0 (len rb)

let tests = [
    "create">::test_create;
    "add">::test_add;
    "make">::test_make;
    "rb_len">::test_len;
    "read">::test_read;
    "read_uint8">::test_uint8;
    "read_int16">::test_int16;
    "read_string">::test_readstr;
    "read_all">::test_readall;
]

end

let tests = ReadBufferTests.tests

end

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
type 'a monad = 'a Lwt.t
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
    timer: int;
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
    | _ -> raise (Invalid_argument "bit not zero or one")


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


    let connect_payload ?userpass ?will ?(flags = []) ?(timer = 10) id =
        let name = addlen "MQIsdp" in
        let version = "\003" in
        if timer > 0xFFFF then raise (Invalid_argument "timer too large");
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
        let tbuf = int16be timer in
        let fbuf = Bytes.create 1 in
        BE.set_int8 fbuf 0 flags;
        let accum acc a = acc + (String.length a) in
        let fields = [name; version; Bytes.to_string fbuf; Bytes.to_string tbuf; pay] in
        let lens = List.fold_left accum 0 fields in
        let buf = Buffer.create lens in
        List.iter (Buffer.add_string buf) fields;
        Buffer.contents buf

    let connect ?userpass ?will ?flags ?timer ?(opt = (false, Atmost_once, false)) id =
        let cxn_pay = connect_payload ?userpass ?will ?flags ?timer id in
        let hdr = fixed_header Connect_pkt opt (String.length cxn_pay) in
        hdr ^ cxn_pay

    let connect_data d =
        let clientid = d.clientid in
        let userpass = d.userpass in
        let will = d.will in
        let flags = d.flags in
        let timer = d.timer in
        connect_payload ?userpass ?will ~flags ~timer clientid

    let connack ?(opt = (false, Atmost_once, false)) ~session_present:_ status =
      let hdr = fixed_header Connack_pkt opt 2 in
      (* FIXME: session_present is not encoded. *)
      let varhdr = Bytes.to_string (connection_status_to_int status |> int16be) in
      hdr ^ varhdr

  end
end



let decode_connect rb =
    let lead = ReadBuffer.read rb 9 in
    if "\000\006MQIsdp\003" <> lead then
        raise (Invalid_argument "invalid MQIsdp or version");
    let hdr = ReadBuffer.read_uint8 rb in
    let timer = ReadBuffer.read_uint16 rb in
    let has_username = 0 <> (hdr land 0x80) in
    let has_password = 0 <> (hdr land 0xC0) in
    let will_flag = bool_of_bit ((hdr land 0x04) lsr 2) in
    let will_retain = will_flag && 0 <> (hdr land 0x20) in
    let will_qos = if will_flag then
        Some (qos_of_bits ((hdr land 0x18) lsr 3)) else None in
    let clean_session = bool_of_bit ((hdr land 0x02) lsr 1) in
    let rs = ReadBuffer.read_string in
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
    Connect {clientid; userpass; will; flags; timer;}

let decode_connack rb =
    let session_present = bool_of_bit (ReadBuffer.read_uint8 rb) in
    let connection_status = connection_status_of_int (ReadBuffer.read_uint8 rb) in
    Connack { session_present; connection_status }

let decode_publish (_, qos, _) rb =
    let topic = ReadBuffer.read_string rb in
    let msgid = if qos = Atleast_once || qos = Exactly_once then
        Some (ReadBuffer.read_uint16 rb)
    else None in
    let payload = ReadBuffer.len rb |> ReadBuffer.read rb in
    Publish (msgid, topic, payload)

let decode_puback rb = Puback (ReadBuffer.read_uint16 rb)

let decode_pubrec rb = Pubrec (ReadBuffer.read_uint16 rb)

let decode_pubrel rb = Pubrel (ReadBuffer.read_uint16 rb)

let decode_pubcomp rb = Pubcomp (ReadBuffer.read_uint16 rb)

let decode_subscribe rb =
    let id = ReadBuffer.read_uint16 rb in
    let get_topic rb =
        let topic = ReadBuffer.read_string rb in
        let qos = ReadBuffer.read_uint8 rb |> qos_of_bits in
        (topic, qos) in
    let topics = ReadBuffer.read_all rb get_topic in
    Subscribe (id, topics)

let decode_suback rb =
    let id = ReadBuffer.read_uint16 rb in
    let get_qos rb = ReadBuffer.read_uint8 rb |> qos_of_bits in
    let qoses = ReadBuffer.read_all rb get_qos in
    Suback (id, List.rev qoses)

let decode_unsub rb =
    let id = ReadBuffer.read_uint16 rb in
    let topics = ReadBuffer.read_all rb ReadBuffer.read_string in
    Unsubscribe (id, topics)

let decode_unsuback rb = Unsuback (ReadBuffer.read_uint16 rb)

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

let read_packet ctx =
    let (inch, _) = ctx in
    Lwt_io.read_char inch >>= fun header_byte ->
    let (msgid, opts) = decode_fixed_header (Char.code header_byte) in
    decode_length inch >>= fun count ->
    let data = Bytes.create count in
    let rd = try Lwt_io.read_into_exactly inch data 0 count
    with End_of_file -> Lwt.fail (Failure "could not read bytes") in
    rd >>= fun () ->
    let pkt = ReadBuffer.make (data |> Bytes.to_string) |> decode_packet opts msgid in
    Lwt.return (opts, pkt)

module MqttTests : sig

    val tests : OUnit.test list

end = struct

let test_encode _ =
    assert_equal 0l (encode_length 0);
    assert_equal 0x7Fl (encode_length 127);
    assert_equal 0x8001l (encode_length 128);
    assert_equal 0xFF7Fl (encode_length 16383);
    assert_equal 0x808001l (encode_length 16384);
    assert_equal 0xFFFF7Fl (encode_length 2097151);
    assert_equal 0x80808001l (encode_length 2097152);
    assert_equal 0xFFFFFF7Fl (encode_length 268435455)

let test_decode_in =
    let equals inp =
        let printer = string_of_int in
        let buf = Bytes.create 4 in
        BE.set_int32 buf 0 (encode_length inp);
        let buf = Bytes.to_string buf in
        let buf = Lwt_bytes.of_string (trunc buf) in
        let inch = Lwt_io.of_bytes ~mode:Lwt_io.input buf in
        decode_length inch >>= fun len ->
        assert_equal ~printer inp len;
        Lwt.return_unit in
    let tests = [0; 127; 128; 16383; 16384; 2097151; 2097152; 268435455] in
    Lwt_list.iter_p equals tests

let test_decode _ = Lwt_main.run test_decode_in

let test_header _ =
    let hdr = fixed_header Disconnect_pkt (true, Exactly_once, true) 99 in
    assert_equal "\237\099" hdr;
    let hdr = fixed_header Connect_pkt (false, Atmost_once, false) 255 in
    assert_equal "\016\255\001" hdr

let test_connect _ =
    let connect_payload = Packet.Encoder.connect_payload in
    let pkt = connect_payload "1" in
    assert_equal "\000\006MQIsdp\003\000\000\n\000\0011" pkt;
    let pkt = connect_payload ~timer:11 "11" in
    assert_equal "\000\006MQIsdp\003\000\000\011\000\00211" pkt;
    let pkt () = connect_payload ~timer:0x10000 "111" in
    assert_raises (Invalid_argument "timer too large") pkt;
    let pkt = connect_payload ~userpass:(Username "bob") "2" in
    assert_equal "\000\006MQIsdp\003\128\000\n\000\0012\000\003bob" pkt;
    let lstr = Bytes.create 0x10000 |> Bytes.to_string in (* long string *)
    let pkt () = connect_payload ~userpass:(Username lstr) "22" in
    assert_raises (Invalid_argument "string too long") pkt;
    let pkt = connect_payload ~userpass:(UserPass ("", "alice")) "3" in
    assert_equal "\000\006MQIsdp\003\192\000\n\000\0013\000\000\000\005alice" pkt;
    let pkt () = connect_payload ~userpass:(UserPass ("", lstr)) "33" in
    assert_raises (Invalid_argument "string too long") pkt;
    let pkt = connect_payload ~will:("a","b") "4" in
    assert_equal "\000\006MQIsdp\003\004\000\n\000\0014\000\001a\000\001b" pkt;
    let pkt () = connect_payload ~will:(lstr,"") "44" in
    assert_raises (Invalid_argument "string too long") pkt;
    let pkt () = connect_payload ~will:("",lstr) "444" in
    assert_raises (Invalid_argument "string too long") pkt;
    let pkt = connect_payload ~will:("", "") ~userpass:(UserPass ("", "")) ~flags:[Will_retain; Will_qos Exactly_once; Clean_session] "5" in
    assert_equal "\000\006MQIsdp\003\246\000\n\000\0015\000\000\000\000\000\000\000\000" pkt

let test_fixed_dec _ =
    let printer p = bits_of_message p |> string_of_int in
    let msg, opt = decode_fixed_header 0x10 in
    let (dup, qos, retain) = opt in
    assert_equal ~printer Connect_pkt msg;
    assert_equal false dup;
    assert_equal Atmost_once qos;
    assert_equal false retain;
    let (msg, opt) = decode_fixed_header 0xED in
    let (dup, qos, retain) = opt in
    assert_equal ~printer Disconnect_pkt msg;
    assert_equal true dup;
    assert_equal Exactly_once qos;
    assert_equal true retain;
    ()

let print_cxn_data = function Connect cd ->
    let clientid = cd.clientid in
    let userpass = opt_with (function Username u -> u | UserPass (u, p) -> u^"_"^p) "none" cd.userpass in
    let will = opt_with (fun (t, m) -> t^"_"^m) "will:none" cd.will in
    let timer =  cd.timer in
    let f2s = function
        | Will_retain -> "retain"
        | Clean_session -> "session"
        | Will_qos qos -> string_of_int (bits_of_qos qos) in
    let flags = String.concat "," (List.map f2s cd.flags) in
    Printf.sprintf "%s %s %s %s %d" clientid userpass will flags timer
    |_ -> ""

let test_cxn_dec _ =
    let printer = print_cxn_data in
    let clientid = "asdf" in
    let userpass = None in
    let will = None in
    let flags = [] in
    let timer = 2000 in
    let d = {clientid; userpass; will; flags; timer} in
    let res = Packet.Encoder.connect_data d |> ReadBuffer.make |> decode_connect in
    assert_equal (Connect d) res;
    let userpass = Some (UserPass ("qwerty", "supersecret")) in
    let will = Some ("topic", "go in peace") in
    let flags = [ Will_retain ; (Will_qos Atleast_once) ; Clean_session] in
    let d = {clientid; userpass; will; flags; timer} in
    let res = Packet.Encoder.connect_data d |> ReadBuffer.make |> decode_connect in
    assert_equal ~printer (Connect d) res

let test_connack _ =
    let all_statuses = [
      Accepted; Unacceptable_protocol_version; Identifier_rejected;
      Server_unavailable; Bad_username_or_password; Not_authorized] in
    let i2rb i = " \002" ^ (int16be i |> Bytes.to_string) in
    List.iteri (fun i status ->
        Packet.Encoder.connack ~session_present:false status |> assert_equal (i2rb i))
      all_statuses

let test_cxnack_dec _ =
    let all_statuses = [
      Accepted; Unacceptable_protocol_version; Identifier_rejected;
      Server_unavailable; Bad_username_or_password; Not_authorized] in
    (* FIXME: This is assuming session present is always set to 0 as part of 16be *)
    let i2rb i = int16be i |> Bytes.to_string |> ReadBuffer.make |> decode_connack in
    List.iteri (fun i connection_status -> i2rb i |> assert_equal (Connack {
      session_present = false;
      connection_status
    })) all_statuses;
    assert_raises (Invalid_argument "connack flag unrecognized") (fun () -> i2rb 7)

let test_pub _ =
    let res = Packet.Encoder.publish "a" "b" in
    let m = "0\004\000\001ab" in
    assert_equal m res;
    let res = Packet.Encoder.publish ~id:7 "a" "b" in
    assert_equal m res;
    let res = Packet.Encoder.publish ~opt:(false, Atleast_once, false) ~id:7 "a" "b" in
    let m = "2\006\000\001a\000\007b" in
    assert_equal m res;
    let res = Packet.Encoder.publish ~opt:(false, Exactly_once, false) ~id:7 "a" "b" in
    let m = "4\006\000\001a\000\007b" in
    assert_equal m res

let test_pub_dec _ =
    let m = "\000\001abcdef" in
    let opt = (false, Atmost_once, false) in
    let res = ReadBuffer.make m |> decode_publish opt in
    let expected = Publish (None, "a", "bcdef") in
    assert_equal expected res;
    let m = "\000\001a\000\007bcdef" in
    let res = ReadBuffer.make m |> decode_publish opt in
    let expected = Publish (None, "a", "\000\007bcdef") in
    assert_equal expected res;
    let opt = (false, Atleast_once, false) in
    let res = ReadBuffer.make m |> decode_publish opt in
    let expected = Publish (Some 7, "a", "bcdef") in
    assert_equal expected res;
    let opt = (false, Exactly_once, false) in
    let res = ReadBuffer.make m |> decode_publish opt in
    assert_equal expected res

let test_puback _ =
    let m = "@\002\000\007" in
    let res = Packet.Encoder.puback 7 in
    assert_equal m res

let test_puback_dec _ =
    let m = "\000\007" in
    let res = ReadBuffer.make m |> decode_puback in
    let expected = Puback 7 in
    assert_equal expected res

let test_pubrec _ =
    let m = "P\002\000\007" in
    let res = pubrec 7 in
    assert_equal m res

let test_pubrec_dec _ =
    let m = "\000\007" in
    let res = ReadBuffer.make m |> decode_pubrec in
    let expected = Pubrec 7 in
    assert_equal expected res

let test_pubrel _ =
    let m = "`\002\000\007" in
    let res = pubrel 7 in
    assert_equal m res;
    let m = "h\002\000\007" in
    let res = pubrel ~opt:(true, Atmost_once, false) 7 in
    assert_equal m res

let test_pubrel_dec _ =
    let m = "\000\007" in
    let res = ReadBuffer.make m |> decode_pubrel in
    let expected = Pubrel 7 in
    assert_equal expected res

let test_pubcomp _ =
    let m = "p\002\000\007" in
    let res = pubcomp 7 in
    assert_equal m res

let test_pubcomp_dec _ =
    let m = "\000\007" in
    let res = ReadBuffer.make m |> decode_pubcomp in
    let expected = Pubcomp 7 in
    assert_equal expected res

let test_subscribe _ =
    let q = ["asdf"; "qwerty"; "poiuy"; "mnbvc"; "zxcvb"] in
    let foo = List.map (fun z -> (z, Atmost_once)) q in
    let res = Packet.Encoder.subscribe ~id:7 foo in
    assert_equal "\130*\000\007\000\004asdf\000\000\006qwerty\000\000\005poiuy\000\000\005mnbvc\000\000\005zxcvb\000" res

let test_sub_dec _ =
    let topics = [("c", Atmost_once); ("b", Atmost_once); ("a", Atmost_once)] in
    let m = "\000\007\000\001a\000\000\001b\000\000\001c\000" in
    let res = ReadBuffer.make m |> decode_subscribe in
    let expected = Subscribe (7, topics) in
    assert_equal expected res

let test_suback _ =
    let s = suback 7 [Atmost_once; Exactly_once; Atleast_once] in
    let m = "\144\005\000\007\000\002\001" in
    assert_equal m s;
    let s = suback 7 [] in
    let m = "\144\002\000\007" in
    assert_equal m s

let test_suback_dec _ =
    let m = "\000\007\000\001\002" in
    let res = ReadBuffer.make m |> decode_suback in
    let expected = Suback (7, [Atmost_once; Atleast_once; Exactly_once]) in
    assert_equal expected res

let test_unsub _ =
    let m = "\162\b\000\007\000\001a\000\001b" in
    let res = unsubscribe ~id:7 ["a";"b"] in
    assert_equal m res

let test_unsub_dec _ =
    let m = "\000\007\000\001a\000\001b" in
    let res = ReadBuffer.make m |> decode_unsub in
    let expected = Unsubscribe (7, ["b";"a"]) in
    assert_equal expected res

let test_unsuback _ =
    let m = "\176\002\000\007" in
    let res = unsuback 7 in
    assert_equal m res

let test_unsuback_dec _ =
    let m = "\000\007" in
    let res = ReadBuffer.make m |> decode_unsuback in
    let expected = Unsuback 7 in
    assert_equal expected res

let test_pingreq _ = assert_equal "\192\000" (pingreq ())

let test_pingreq_dec _ =
    assert_equal Pingreq (ReadBuffer.make "" |> decode_pingreq)

let test_pingresp _ = assert_equal "\208\000" (pingresp ())

let test_pingresp_dec _ =
    assert_equal Pingresp (ReadBuffer.make "" |> decode_pingresp)

let test_disconnect _ = assert_equal "\224\000" (Packet.Encoder.disconnect ())

let test_disconnect_dec _ =
    assert_equal Disconnect (ReadBuffer.make "" |> decode_disconnect)

let tests = [
    "encode">::test_encode;
    "decode">::test_decode;
    "hdr">::test_header;
    "connect">::test_connect;
    "decode fixed">::test_fixed_dec;
    "decode_cxn">::test_cxn_dec;
    "connack">::test_connack;
    "decode_cxnack">::test_cxnack_dec;
    "publish">::test_pub;
    "decode_pub">::test_pub_dec;
    "puback">::test_puback;
    "decode_puback">::test_puback_dec;
    "pubrec">::test_pubrec;
    "decode_pubrec">::test_pubrec_dec;
    "pubrel">::test_pubrel;
    "decode_pubrel">::test_pubrel_dec;
    "pubcomp">::test_pubcomp;
    "decode_pubcomp">::test_pubcomp_dec;
    "subscribe">::test_subscribe;
    "decode_sub">::test_sub_dec;
    "suback">::test_suback;
    "decode_suback">::test_suback_dec;
    "unsub">::test_unsub;
    "decode_unsub">::test_unsub_dec;
    "unsuback">::test_unsuback;
    "decode_unsuback">::test_unsuback_dec;
    "pingreq">::test_pingreq;
    "decode_pingreq">::test_pingreq_dec;
    "pingresp">::test_pingresp;
    "decode_pingresp">::test_pingresp_dec;
    "disconnect">::test_disconnect;
    "decode_disc">::test_disconnect_dec;
]

end

let tests = ReadBuffer.tests @ MqttTests.tests

module MqttClient = struct

    type client = {
        cxn : t;
        stream: (string * string) Lwt_stream.t;
        push : ((string * string) option -> unit);
        inflight : (int, (unit Lwt_condition.t * packet)) Hashtbl.t;
        mutable reader : unit Lwt.t;
        mutable pinger : unit Lwt.t;
        error_fn : (client -> exn -> unit Lwt.t);
    }

    let default_error_fn _client exn =
        Lwt_io.printlf "mqtt error: %s\n%s" (Printexc.to_string exn) (Printexc.get_backtrace ())

    let connect_options ?(clientid = "OCamlMQTT") ?userpass ?will ?(flags= [Clean_session]) ?(timer = 10) () =
        { clientid; userpass; will; flags; timer}

    let read_packets client () =
        let ((_in_chan, out_chan) as cxn) = client.cxn in

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
          read_packet cxn >>= fun ((_dup, qos, _retain), packet) -> begin
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

            | Pingresp -> Lwt.return_unit

            | _ -> Lwt.fail (Failure "unknown packet from server")
          end >>= loop
        in
        loop ()

    let wrap_catch client f = Lwt.catch f (client.error_fn client)

    let pinger cxn timeout () =
        let (_, oc) = cxn in
        let tmo = 0.9 *. (float_of_int timeout) in (* 10% leeway *)
        let rec loop g =
            Lwt_unix.sleep tmo >>= fun () ->
            pingreq () |> Lwt_io.write oc >>= fun () ->
            loop g in
        loop ()


    let () = Printexc.record_backtrace true


    let connect ?(opt = connect_options ()) ?(error_fn = default_error_fn) ?(port = 1883) host =
      (* Estabilish a socket connection. *)
      Lwt_unix.getaddrinfo host (string_of_int port) [] >>= fun addresses ->
      let sockaddr = Lwt_unix.((List.hd addresses).ai_addr) in
      Lwt_io.open_connection sockaddr >>= fun ((_ic, oc) as connection) ->

      (* Send the CONNECT packet to the server. *)
      let connect_packet = Packet.Encoder.connect
          ?userpass:opt.userpass
          ?will:opt.will
          ~flags:opt.flags
          ~timer:opt.timer
          opt.clientid
      in
      Lwt_io.write oc connect_packet >>= fun () ->

      let stream, push = Lwt_stream.create () in
      let inflight = Hashtbl.create 100 in
      read_packet connection >>= fun packet ->
        match packet with
        | (_, Connack { connection_status = Accepted; session_present }) ->
          Lwt_io.printlf "[DEBUG] Mqtt: Connected session_present=%b"
              session_present >>= fun () ->
          let ping = Lwt.return_unit in
          let reader = Lwt.return_unit in

          let client = { cxn = connection; stream; push; inflight; reader; pinger=ping; error_fn; } in
          (* let pinger = wrap_catch client (pinger connection opt.timer) in *)
          (* let reader = wrap_catch client (read_packets client) in *)

          let pinger = pinger connection opt.timer () in
          let reader = read_packets client () in

          Lwt.async (fun () ->
            Lwt.catch (fun () -> pinger <&> reader)
            (function
              | Lwt.Canceled ->
                (* Lwt_io.printl "[DEBUG] Mqtt_client: Stopped client thread." >>= fun () -> *)
                Lwt.return_unit
              | exn -> Lwt.fail exn));

          client.pinger <- pinger;
          client.reader <- reader;

          Lwt.return client

        | (_, Connack pkt) ->
          Lwt.fail (Failure (connection_status_to_string pkt.connection_status))
        | _ -> Lwt.fail (Failure ("Unknown packet type received after conn"))


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
        let mid = !msgid in
        let cond = Lwt_condition.create () in
        Hashtbl.add client.inflight mid (cond, (Suback (mid, qos_list)));
        wrap_catch client (fun () ->
        Lwt_io.write oc subscribe_packet >>= fun () ->
        Lwt_condition.wait cond >>= fun _ ->
        Lwt.return_unit)


    (* TODO: push None to client; stop reader and pinger ?? *)
    let disconnect client =
      let (ic, oc) = client.cxn in

      (* Terminate the packet stream. *)
      client.push None;

      (* Cancel the reader and pinger threads. *)
      Lwt.cancel client.reader;
      Lwt.cancel client.pinger;

      (* Send the disconnect packet to server. *)
      Lwt_io.write oc (Packet.Encoder.disconnect ()) >>= fun () ->

      (* Close the connection. *)
      Lwt_io.flush oc >>= fun () ->
      Lwt_io.close ic >>= fun () ->
      Lwt_io.close oc


    let sub_stream client = client.stream

end

module MqttServer = struct

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
    read_packet cxn >>= (function
    | (_, Connect _) ->
      let connack = Packet.Encoder.connack ~session_present:false Accepted in
      Lwt_io.write outch connack
    | _ -> Lwt.fail (Failure "Mqtt Server: Expected connect")) >>= fun () ->
    let rec loop g =
        read_packet cxn >>= (function
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

end
