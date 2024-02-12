module C = Mqtt_client

let host = "127.0.0.1"
let port = 1883

let sub_example () =
  let on_message ~topic payload = Lwt_io.printlf "%s: %s" topic payload in
  let%lwt () = Lwt_io.printl "Starting subscriber..." in
  let%lwt client = C.connect ~on_message ~id:"client-1" ~port [ host ] in
  C.subscribe [ ("topic-1", C.Atmost_once) ] client

let[@warning "-unused-value-declaration"] pub_example () =
  let%lwt () = Lwt_io.printl "Starting publisher..." in
  let%lwt client = C.connect ~id:"client-1" ~port [ host ] in
  let rec loop () =
    let%lwt () = Lwt_io.printl "Publishing..." in
    let%lwt line = Lwt_io.read_line Lwt_io.stdin in
    let%lwt () = C.publish ~qos:C.Atleast_once ~topic:"topic-1" line client in
    let%lwt () = Lwt_io.printl "Published." in
    loop ()
  in
  loop ()

let () = Lwt_main.run (sub_example ())
