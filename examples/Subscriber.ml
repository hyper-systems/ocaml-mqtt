module Mqtt_client = Mqtt.Mqtt.MqttClient
let (>>=) = Lwt.bind

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
  let opt = Mqtt_client.connect_options ~keep_alive:10 ~flags:[] ~clientid:"client-1" () in
  Mqtt_client.connect ~opt "localhost" >>= fun client ->
  Mqtt_client.subscribe client [("topic-1", Mqtt.Mqtt.Atmost_once)] >>= fun () ->
  let stream = Mqtt_client.sub_stream client in
  loop stream

let () = Lwt_main.run (sub_example ())

