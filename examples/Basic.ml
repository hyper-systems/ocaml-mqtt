module C = Mqtt_client
let (>>=) = Lwt.bind

let host = "localhsot"
let port = 1883

let sub_example () =
  let rec loop stream =
    Lwt_stream.get stream >>= function
    | None ->
      Lwt_io.printl "STREAM FINISHED"
    | Some (t, p) ->
      Lwt_io.printlf "%s: %s" t p >>= fun () ->
      loop stream
  in
  let%lwt () = Lwt_io.printl "Starting subscriber...";
  let%lwt client = C.connect ~id:"client-1" ~port [host];
  C.subscribe [("topic-1", C.Atmost_once)] client >>= fun () ->
  let stream = C.messages client in
  loop stream


let pub_example () =
  Lwt_io.printl "Starting publisher..." >>= fun () ->
  C.connect ~id:"client-1" ~port [host] >>= fun client ->
  let rec loop () =
    Lwt_io.printl "Publishing..." >>= fun () ->
    Lwt_io.read_line Lwt_io.stdin >>= fun line ->
    C.publish ~qos:C.Atleast_once ~topic:"topic-1" line client >>= fun () ->
    Lwt_io.printl "Published." >>= fun () ->
    loop ()
  in
  loop ()


let () = Lwt_main.run (sub_example ())

