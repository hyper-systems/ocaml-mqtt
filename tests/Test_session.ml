module Mqtt_client = Mqtt.Mqtt.MqttClient
module Mqtt = Mqtt.Mqtt
let (>>=) = Lwt.bind
let (<&>) = Lwt.(<&>)


let sub () =
  let connect () =
    let clientid = "sub-client-1" in
    let opt = Mqtt_client.connect_options ~flags:[] ~clientid () in
    Mqtt_client.connect ~opt "localhost" >>= fun client ->
    Mqtt_client.subscribe client [("topic-1", Mqtt.Atleast_once)] >>= fun () ->
    Lwt.return client
  in
  let reconnect client =
    Mqtt_client.disconnect client >>= fun () ->
    Lwt_unix.sleep 5.0 >>= fun () ->
    connect ()
  in
  connect () >>= fun client ->
  let stream = Mqtt_client.sub_stream client in
  Lwt_stream.get stream >>= function
    | Some (_topic, payload) ->
      assert (payload = "msg-1");
      reconnect client >>= fun client ->
      let stream = Mqtt_client.sub_stream client in
      Lwt_stream.get stream >>= (function
        | Some (_topic, payload) ->
          assert (payload = "msg-2");
          Lwt_stream.get stream >>= (function
            | Some (_topic, payload) ->
              assert (payload = "msg-3");
              Mqtt_client.disconnect client
            | None ->
              assert false)
        | None ->
          assert false)
    | None ->
      assert false


let pub () =
  let clientid = "pub-client-1" in
  let opt = Mqtt_client.connect_options ~clientid () in
  Mqtt_client.connect ~opt "localhost" >>= fun client ->
  let qos = Mqtt.Atleast_once in
  Mqtt_client.publish ~qos client "topic-1" "msg-1" >>= fun () ->
  (* Give some time to the subscriber to disconnect. *)
  Lwt_unix.sleep 1.0 >>= fun () ->
  Mqtt_client.publish ~qos client "topic-1" "msg-2" >>= fun () ->
  Mqtt_client.publish ~qos client "topic-1" "msg-3" >>= fun () ->
  Mqtt_client.disconnect client


let () =
  Lwt_main.run (sub () <&> pub ())


