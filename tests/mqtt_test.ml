open OUnit
open Subscriptions

let (>>=) = Lwt.(>>=)

open Mqtt

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
  let pkt = connect_payload ~keep_alive:11 "11" in
  assert_equal "\000\006MQIsdp\003\000\000\011\000\00211" pkt;
  let pkt () = connect_payload ~keep_alive:0x10000 "111" in
  assert_raises (Invalid_argument "keep_alive too large") pkt;
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
  let keep_alive =  cd.keep_alive in
  let f2s = function
    | Will_retain -> "retain"
    | Clean_session -> "session"
    | Will_qos qos -> string_of_int (bits_of_qos qos) in
  let flags = String.concat "," (List.map f2s cd.flags) in
  Printf.sprintf "%s %s %s %s %d" clientid userpass will flags keep_alive
                            |_ -> ""

let test_cxn_dec _ =
  let printer = print_cxn_data in
  let clientid = "asdf" in
  let userpass = None in
  let will = None in
  let flags = [] in
  let keep_alive = 2000 in
  let d = {clientid; userpass; will; flags; keep_alive} in
  let res = Packet.Encoder.connect_data d |> Read_buffer.make |> decode_connect in
  assert_equal (Connect d) res;
  let userpass = Some (UserPass ("qwerty", "supersecret")) in
  let will = Some ("topic", "go in peace") in
  let flags = [ Will_retain ; (Will_qos Atleast_once) ; Clean_session] in
  let d = {clientid; userpass; will; flags; keep_alive} in
  let res = Packet.Encoder.connect_data d |> Read_buffer.make |> decode_connect in
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
  let test (session_present, connection_status) =
    let expected = Connack { session_present; connection_status } in
    let data = Packet.Encoder.connack ~session_present connection_status in
    let rb = Read_buffer.make data in
    let header_byte = Read_buffer.read_uint8 rb in
    let (pkt_type, (dup, qos, retain)) = decode_fixed_header header_byte in
    assert_equal pkt_type Connack_pkt;
    assert_equal (dup, qos, retain) (false, Atmost_once, false);
    let len = Read_buffer.read_uint8 rb in
    assert_equal len 2;
    let actual = decode_connack rb in
    assert_equal actual expected in
  List.iter test [
    (false, Accepted);
    (false, Unacceptable_protocol_version);
    (true, Identifier_rejected);
    (false, Server_unavailable);
    (true, Bad_username_or_password);
    (false, Not_authorized)
  ]

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
  let res = Read_buffer.make m |> decode_publish opt in
  let expected = Publish (None, "a", "bcdef") in
  assert_equal expected res;
  let m = "\000\001a\000\007bcdef" in
  let res = Read_buffer.make m |> decode_publish opt in
  let expected = Publish (None, "a", "\000\007bcdef") in
  assert_equal expected res;
  let opt = (false, Atleast_once, false) in
  let res = Read_buffer.make m |> decode_publish opt in
  let expected = Publish (Some 7, "a", "bcdef") in
  assert_equal expected res;
  let opt = (false, Exactly_once, false) in
  let res = Read_buffer.make m |> decode_publish opt in
  assert_equal expected res

let test_puback _ =
  let m = "@\002\000\007" in
  let res = Packet.Encoder.puback 7 in
  assert_equal m res

let test_puback_dec _ =
  let m = "\000\007" in
  let res = Read_buffer.make m |> decode_puback in
  let expected = Puback 7 in
  assert_equal expected res

let test_pubrec _ =
  let m = "P\002\000\007" in
  let res = pubrec 7 in
  assert_equal m res

let test_pubrec_dec _ =
  let m = "\000\007" in
  let res = Read_buffer.make m |> decode_pubrec in
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
  let res = Read_buffer.make m |> decode_pubrel in
  let expected = Pubrel 7 in
  assert_equal expected res

let test_pubcomp _ =
  let m = "p\002\000\007" in
  let res = pubcomp 7 in
  assert_equal m res

let test_pubcomp_dec _ =
  let m = "\000\007" in
  let res = Read_buffer.make m |> decode_pubcomp in
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
  let res = Read_buffer.make m |> decode_subscribe in
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
  let res = Read_buffer.make m |> decode_suback in
  let expected = Suback (7, [Atmost_once; Atleast_once; Exactly_once]) in
  assert_equal expected res

let test_unsub _ =
  let m = "\162\b\000\007\000\001a\000\001b" in
  let res = unsubscribe ~id:7 ["a";"b"] in
  assert_equal m res

let test_unsub_dec _ =
  let m = "\000\007\000\001a\000\001b" in
  let res = Read_buffer.make m |> decode_unsub in
  let expected = Unsubscribe (7, ["b";"a"]) in
  assert_equal expected res

let test_unsuback _ =
  let m = "\176\002\000\007" in
  let res = unsuback 7 in
  assert_equal m res

let test_unsuback_dec _ =
  let m = "\000\007" in
  let res = Read_buffer.make m |> decode_unsuback in
  let expected = Unsuback 7 in
  assert_equal expected res

let test_pingreq _ = assert_equal "\192\000" (pingreq ())

let test_pingreq_dec _ =
  assert_equal Pingreq (Read_buffer.make "" |> decode_pingreq)

let test_pingresp _ = assert_equal "\208\000" (pingresp ())

let test_pingresp_dec _ =
  assert_equal Pingresp (Read_buffer.make "" |> decode_pingresp)

let test_disconnect _ = assert_equal "\224\000" (Packet.Encoder.disconnect ())

let test_disconnect_dec _ =
  assert_equal Disconnect (Read_buffer.make "" |> decode_disconnect)

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

let _ =
    let tests = Read_buffer.tests @ tests @ Subscriptions.tests in
    let suite = "mqtt">:::tests in
    run_test_tt_main suite


