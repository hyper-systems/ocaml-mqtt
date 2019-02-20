let (>>=) = Lwt.bind
let (<&>) = Lwt.(<&>)

module Mqtt = Mqtt.Mqtt


module Client = struct
  module Mc = Mqtt.MqttClient

  let rec read_messages stream =
    Lwt_stream.get stream >>= function
    | None -> Lwt_io.printl("client: message stream empty")
    | Some (t, p) -> Lwt_io.printlf "client: got message «%s: %s»" t p >>= fun () ->
      read_messages stream

  let start ?(host = "localhost") () =
    Lwt_io.printl "client: starting..." >>= fun () ->
    let opt = Mc.options ~keep_alive:5 ~ping_timeout:3. ~flags:[] ~clientid:"client-1" () in
    let%lwt client = Mc.connect ~opt host in
    let%lwt () = Mc.subscribe client ["topic-1", Mqtt.Atmost_once] in
    Lwt_io.printl "client: subscribed" >>= fun () ->
    let stream = Mc.sub_stream client in
    read_messages stream
end


module Server = struct
  exception Stop

  let on_listen _address (in_channel, out_channel) =
    Lwt_io.printl "server: connection estabilished" >>= fun () ->
    let ping_count = ref 0 in
    let ping_limit = 3 in

    let%lwt () =
      match%lwt Mqtt.read_packet in_channel with
      | (_, Connect _) ->
        let connack = Mqtt.Packet.Encoder.connack ~session_present:false Accepted in
        Lwt_io.write out_channel connack
      | _ -> Lwt.fail (Failure "server: expected connect packet") in

    let rec loop () =
      let%lwt () =
        match%lwt Mqtt.read_packet in_channel with
        | (_, Publish _) -> Lwt_io.printl "server: publish request"
        | (_, Subscribe (id, qos_list)) ->
          let%lwt () = Lwt_io.printl "server: subscribe request" in
          let connack = Mqtt.suback id (List.map snd qos_list) in
          Lwt_io.write out_channel connack

        | (_, Disconnect) ->
          let%lwt () = Lwt_io.printl "server: disconnect request" in
          Lwt.fail Stop
        | (_, Pingreq) when !ping_count >= ping_limit ->
          Lwt_io.printl "server: ping request, ignoring..."
        | (_, Pingreq) ->
          Lwt_io.printl "server: ping request, sending response..." >>= fun () ->
          Lwt_io.write out_channel (Mqtt.pingresp ()) >>= fun () ->
          incr ping_count;
          Lwt.return_unit

        | _ -> Lwt.fail (Failure "server: unknown packet") in
      loop ()
    in
    Lwt.catch loop
      (function
       | Stop -> Lwt_io.printl "server: stopped (idle connection)"
       | exn -> Lwt.fail exn)


  let addr host port =
    Lwt_unix.gethostbyname host >>= fun hostent ->
    let inet_addr = hostent.Unix.h_addr_list.(0) in
    Unix.ADDR_INET (inet_addr, port) |> Lwt.return


  let start ?(host = "localhost") ?(port = 1883) () =
    addr host port >>= fun a ->
    Lwt_io.printl "server: starting..." >>= fun () ->
    Lwt_io.establish_server_with_client_address ~backlog:1000 a on_listen
end






let () =
  Lwt_main.run begin
    match Sys.argv with
    | [|_; "server"|] ->
      ignore (Server.start ());
      Lwt.pick []
    | [|_; "client"|] -> Client.start ()
    | _ -> Lwt_io.printlf "usage: %s <client|server>" Sys.argv.(0)
  end

