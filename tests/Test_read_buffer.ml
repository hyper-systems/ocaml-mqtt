open Mqtt_client.Read_buffer

let test_create _ =
  let rb = create () in
  assert_equal 0 rb.pos;
  assert_equal Bytes.empty rb.buf

let test_add _ =
  let rb = create () in
  add_string rb "asdf";
  assert_equal (Bytes.of_string "asdf") rb.buf;
  add_string rb "qwerty";
  assert_equal (Bytes.of_string "asdfqwerty") rb.buf;
  (* test appends via manually resetting pos *)
  rb.pos <- 4;
  add_string rb "poiuy";
  assert_equal (Bytes.of_string "qwertypoiuy") rb.buf;
  assert_equal 0 rb.pos

let test_make _ =
  let rb = make "asdf" in
  assert_equal 0 rb.pos;
  assert_equal (Bytes.of_string "asdf") rb.buf

let test_len _ =
  let rb = create () in
  assert_equal 0 (len rb);
  add_string rb "asdf";
  assert_equal 4 (len rb);
  let _ = read rb 2 in
  assert_equal 2 (len rb);
  let _ = read rb 2 in
  assert_equal 0 (len rb)

let test_read _ =
  let rb = create () in
  let exn = Invalid_argument "buffer underflow" in
  assert_raises exn (fun () -> read rb 1);
  let rb = make "asdf" in
  assert_raises exn (fun () -> read rb (-1));
  assert_equal "" (read rb 0);
  assert_equal "as" (read rb 2);
  assert_raises exn (fun () -> read rb 3);
  assert_equal 2 rb.pos;
  assert_equal "df" (read rb 2);
  assert_raises exn (fun () -> read rb 1);
  assert_equal 4 rb.pos

let test_uint8 _ =
  let printer = string_of_int in
  let rb = create () in
  let exn = Invalid_argument "string too short" in
  assert_raises exn (fun () -> read_uint8 rb);
  let rb = make "\001\002\255" in
  assert_equal 1 (read_uint8 rb);
  assert_equal 1 rb.pos;
  assert_equal 2 (read_uint8 rb);
  assert_equal 2 rb.pos;
  assert_equal ~printer 255 (read_uint8 rb)

let test_int16 _ =
  let printer = string_of_int in
  let rb = create () in
  let exn = Invalid_argument "string too short" in
  assert_raises exn (fun () -> read_uint16 rb);
  let rb = make "\001" in
  assert_raises exn (fun () -> read_uint16 rb);
  let rb = make "\001\002" in
  assert_equal 258 (read_uint16 rb);
  let rb = make "\255\255\128" in
  assert_equal ~printer 65535 (read_uint16 rb)

let test_readstr _ =
  let rb = create () in
  let exn1 = Invalid_argument "string too short" in
  let exn2 = Invalid_argument "buffer underflow" in
  assert_raises exn1 (fun () -> read_string rb);
  let rb = make "\000" in
  assert_raises exn1 (fun () -> read_string rb);
  let rb = make "\000\001" in
  assert_raises exn2 (fun () -> read_string rb);
  let rb = make "\000\004asdf\000\006qwerty" in
  assert_equal "asdf" (read_string rb);
  assert_equal 6 rb.pos;
  assert_equal "qwerty" (read_string rb);
  assert_equal 14 rb.pos

let test_readall _ =
  let rb = make "\001\002\003\004\005" in
  let res = read_all rb read_uint8 in
  assert_equal res [ 5; 4; 3; 2; 1 ];
  assert_equal 0 (len rb)

let tests =
  [
    "create" >:: test_create;
    "add" >:: test_add;
    "make" >:: test_make;
    "rb_len" >:: test_len;
    "read" >:: test_read;
    "read_uint8" >:: test_uint8;
    "read_int16" >:: test_int16;
    "read_string" >:: test_readstr;
    "read_all" >:: test_readall;
  ]
