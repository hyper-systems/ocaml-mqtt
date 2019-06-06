module Mqtt_client = Mqtt.Client
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
  Lwt_io.printl "Starting subscriber..." >>= fun () ->
  Mqtt_client.connect ~id:"client-1" ~port host >>= fun client ->
  Mqtt_client.subscribe client [("topic-1", Mqtt.Atmost_once)] >>= fun () ->
  let stream = Mqtt_client.messages client in
  loop stream


let pub_example () =
  Lwt_io.printl "Starting publisher..." >>= fun () ->
  Mqtt_client.connect ~id:"client-1" ~port host >>= fun client ->
  let rec loop () =
    Lwt_io.printl "Publishing..." >>= fun () ->
    Lwt_io.read_line Lwt_io.stdin >>= fun line ->
    Mqtt_client.publish client ~qos:Mqtt.Atleast_once "topic-1" line >>= fun () ->
    Lwt_io.printl "Published." >>= fun () ->
    loop ()
  in
  loop ()


let () = Lwt_main.run (sub_example ())

