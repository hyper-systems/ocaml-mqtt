module C = Mqtt_client

let host = "127.0.0.1"
let port = 1883

let sub_example () =
  let rec loop stream =
    let%lwt msg = Lwt_stream.get stream in
    match msg with
    | None ->
      Lwt_io.printl "STREAM FINISHED"
    | Some (t, p) ->
      let%lwt () = Lwt_io.printlf "%s: %s" t p in
      loop stream
  in
  let%lwt () = Lwt_io.printl "Starting subscriber..." in
  let%lwt client = C.connect ~id:"client-1" ~port [host] in
  let%lwt () = C.subscribe [("topic-1", C.Atmost_once)] client in
  let stream = C.messages client in
  loop stream


let pub_example () =
  let%lwt () = Lwt_io.printl "Starting publisher..." in
  let%lwt client = C.connect ~id:"client-1" ~port [host] in
  let rec loop () =
    let%lwt () = Lwt_io.printl "Publishing..." in
    let%lwt line = Lwt_io.read_line Lwt_io.stdin in
    let%lwt () = C.publish ~qos:C.Atleast_once ~topic:"topic-1" line client in
    let%lwt () = Lwt_io.printl "Published." in
    loop ()
  in
  loop ()


let () = Lwt_main.run (sub_example ())

