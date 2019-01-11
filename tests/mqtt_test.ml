open OUnit
open Mqtt
open Subscriptions

let _ =
    let tests = Mqtt.tests @ Subscriptions.tests in
    let suite = "mqtt">:::tests in
    run_test_tt_main suite


