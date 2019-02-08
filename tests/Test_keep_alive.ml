module Mqtt_client = Mqtt.Mqtt.MqttClient
module Mqtt = Mqtt.Mqtt
let (>>=) = Lwt.bind
let (<&>) = Lwt.(<&>)

let on_listen _address ((in_channel: Lwt_io.input_channel), (out_channel: Lwt_io.output_channel)) =
  let open Mqtt in

  let ping_count = ref 0 in
  let ping_limit = 3 in

  let%lwt () = 
    match%lwt read_packet in_channel with
    | (_, Connect _) ->
      let connack = Packet.Encoder.connack ~session_present:false Accepted in
      Lwt_io.write out_channel connack
    | _ -> Lwt.fail (Failure "Mqtt Server: Expected connect") in

  let rec loop () =
    let%lwt () =
      match%lwt read_packet in_channel with
      | (_, Publish _) -> Lwt_io.printl "Publish"
      | (_, Subscribe (id, qos_list)) ->
        let%lwt () = Lwt_io.printl "Subscribe" in
        let connack = suback id (List.map snd qos_list) in
        Lwt_io.write out_channel connack

      | (_, Disconnect) -> Lwt_io.printl "Disconnect"
      | (_, Pingreq) when !ping_count >= ping_limit ->
        Lwt_io.printl "Mqtt Server: Stopping PINGRESP" >>= loop
      | (_, Pingreq) ->
        Lwt_io.printl "Mqtt Server: Pingreq received" >>= fun () ->
        Lwt_io.write out_channel (pingresp ()) >>= fun () ->
        incr ping_count;
        Lwt.return_unit

      | _ -> Lwt.fail (Failure "Mqtt Server: Unknown packet") in
    loop ()
  in
  loop ()

let addr host port =
  Lwt_unix.gethostbyname host >>= fun hostent ->
  let inet_addr = hostent.Unix.h_addr_list.(0) in
  Unix.ADDR_INET (inet_addr, port) |> Lwt.return

let listen ?(host = "localhost") ?(port = 1883) () =
  addr host port >>= fun a ->
  Lwt_io.establish_server_with_client_address ~backlog:1000 a on_listen

let rec read_client_stream stream =
  Lwt_stream.get stream >>= function
  | None -> Lwt_io.printl("Mqtt client: Stream finished")
  | Some (t, p) -> Lwt_io.printlf "Mqtt client: %s: %s" t p >>= fun () ->
    read_client_stream stream

let () =
  Lwt_main.run begin
    ignore (listen ());

    Lwt_io.printl "Mqtt client: Starting subscriber" >>= fun () ->
    let opt = Mqtt_client.options ~keep_alive:5 ~ping_timeout:3. ~flags:[] ~clientid:"client-1" () in
    let%lwt client = Mqtt_client.connect ~opt "localhost" in
    let%lwt () = Mqtt_client.subscribe client ["topic-1", Mqtt.Atmost_once] in
    let stream = Mqtt_client.sub_stream client in
    read_client_stream stream
  end
