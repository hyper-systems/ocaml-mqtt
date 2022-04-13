open OUnit

module Subscriptions : sig
  type 'a t

  val empty : 'a t
  val add_node : string -> 'a -> 'a t -> 'a t
  val remove_node : string -> 'a -> 'a t -> 'a t
  val query : string -> 'a t -> 'a list
  val length : 'a t -> int
  val tests : OUnit.test list
end = struct
  type 'a t =
    | E (* empty *)
    | NV of string * (string, 'a t) Hashtbl.t (* no value     *)
    | V of string * (string, 'a t) Hashtbl.t * 'a list (* value   *)
    | Pound of 'a list
  (* wildcard #   *)

  let split str =
    let strs = ref [] in
    let prev = ref 0 in
    let split_segment i c =
      if c = '/' then (
        let newstr = String.sub str !prev (i - !prev) in
        prev := i + 1;
        (* skip slash *)
        strs := newstr :: !strs)
    in
    String.iteri split_segment str;
    (* fixup the last element *)
    strs := String.sub str !prev (String.length str - !prev) :: !strs;
    List.rev !strs

  let empty = E
  let _tbl_keys tbl = Hashtbl.fold (fun k _ acc -> k :: acc) tbl []
  let tbl_vals tbl = Hashtbl.fold (fun _ v acc -> v :: acc) tbl []

  let rec length = function
    | E -> 0
    | Pound _ -> 1
    | NV (_, t) | V (_, t, _) ->
      let children = tbl_vals t in
      1 + List.fold_left (fun acc x -> acc + length x) 0 children

  let _get_vals = function E | NV _ -> [] | V (_, _, v) | Pound v -> v

  let find_branches tbl k : 'a t list =
    let f x = try [ Hashtbl.find tbl x ] with Not_found -> [] in
    List.map f [ k; "+"; "#" ] |> List.concat

  let pound_lookahead t v =
    try
      match Hashtbl.find t "#" with
      | Pound z -> v @ z
      | _ -> failwith "should never happen"
    with Not_found -> v

  let query key tree =
    let k_parts = split key in
    let rec inner tree v p : 'a list =
      match p with
      | [] -> (
        match tree with
        | Pound z -> v @ z
        | V (_, t, z) -> pound_lookahead t (v @ z)
        | NV (_, t) -> pound_lookahead t v
        | E -> v)
      | h :: m -> (
        match tree with
        | E -> v
        | Pound z -> v @ z
        | NV (_, t) | V (_, t, _) ->
          let branches = find_branches t h in
          let r = List.map (fun x -> inner x v m) branches in
          v @ List.concat r)
    in
    inner tree [] k_parts

  let add_node keys v t =
    let new_node k v i n =
      let h = Hashtbl.create 10 in
      let j = k.(i) in
      if j = "#" then Pound [] else if n <> 0 then NV (j, h) else V (j, h, [ v ])
    in

    let rec add k v i n = function
      | E -> new_node [| "*root*" |] v i n |> add k v i n
      | NV (key, h) as e ->
        if n = 0 then V (key, h, [ v ])
        else
          let child =
            try Hashtbl.find h k.(i) with Not_found -> new_node k v i n
          in
          let child = add k v (i + 1) (n - 1) child in
          Hashtbl.replace h k.(i) child;
          e
      | V (key, h, v1) as e ->
        if n = 0 then V (key, h, v :: v1)
        else
          let child =
            try Hashtbl.find h k.(i) with Not_found -> new_node k v i n
          in
          let child = add k v (i + 1) (n - 1) child in
          Hashtbl.replace h k.(i) child;
          e
      | Pound v1 -> Pound (v :: v1)
    in

    let k = split keys |> Array.of_list in
    add k v 0 (Array.length k) t

  let get_branches = function
    | E | Pound _ -> []
    | V (_, t, _) | NV (_, t) -> tbl_vals t

  let rec get_value = function
    | E -> []
    | NV (_, t) -> List.concat (List.map get_value (tbl_vals t))
    | Pound v -> v
    | V (_, t, v) -> v @ List.concat (List.map get_value (tbl_vals t))

  let find_branch k = function
    | E | Pound _ -> None
    | V (_, t, _) | NV (_, t) -> (
      try Some (Hashtbl.find t k) with Not_found -> None)

  let remove_branch k = function
    | (E as e) | (Pound _ as e) -> e
    | (NV (_, t) as e) | (V (_, t, _) as e) ->
      Hashtbl.remove t k;
      if 0 = Hashtbl.length t then E else e

  let replace_branch k g = function
    | (E as e) | (Pound _ as e) -> e
    | (NV (_, t) as e) | (V (_, t, _) as e) ->
      Hashtbl.replace t k g;
      e

  let remove_node key value tree =
    let parts = split key in
    let remove values v = List.filter (fun x -> x <> v) values in
    let rec inner tree = function
      | [ h ] -> (
        match find_branch h tree with
        | None -> tree
        | Some b -> (
          match b with
          | E | NV _ -> tree
          | Pound v ->
            if "#" = h then
              let v = remove v value in
              if 0 = List.length v then remove_branch h tree
              else replace_branch h (Pound v) tree
            else failwith "not pound; should never happen"
          | V (k, t, v) ->
            let v = remove v value in
            if 0 = List.length v then
              if 0 = Hashtbl.length t then remove_branch k tree
              else replace_branch k (NV (k, t)) tree
            else replace_branch k (V (k, t, v)) tree))
      | h :: t -> (
        match find_branch h tree with
        | Some b ->
          let e = inner b t in
          if E = e then remove_branch h tree else replace_branch h e tree
        | None -> tree)
      | [] -> tree
    in
    inner tree parts

  let rec _tree_of_string tree level =
    let vals = match tree with E | NV _ -> [] | V (_, _, v) | Pound v -> v in
    let key =
      match tree with
      | E -> "*empty*"
      | Pound _ -> "#"
      | NV (k, _) -> "nv: " ^ k
      | V (k, _, _) -> "v: " ^ k
    in
    let branches =
      match tree with
      | E | Pound _ -> []
      | NV (_, t) | V (_, t, _) -> tbl_vals t
    in
    let vals = String.concat "," vals in
    let vals = if "" <> vals then ": " ^ vals else "" in
    let _ = Printf.printf "%*d %s %s\n" level level key vals in
    let func x = _tree_of_string x (level + 4) in
    List.iter func branches

  let split_test _ =
    let printer = String.concat "," in
    let ae = assert_equal ~printer in
    let res = split "a/b/c/d" in
    ae [ "a"; "b"; "c"; "d" ] res;
    let res = split "/abc//def/ghi/" in
    ae [ ""; "abc"; ""; "def"; "ghi"; "" ] res

  let plus_test _ =
    let tree =
      add_node "helo/happy/world!" "helosadworld" empty
      |> add_node "helo/pretty/wurld" "helothere"
      |> add_node "omg/wtf" "omgwtf"
      |> add_node "omg/wtf/bbq" "omgwtfbbq"
      |> add_node "omg/srsly" "omgsrsly"
      |> add_node "omg/+/bbqz" "omgwildbbq"
      |> add_node "omg/+" "omgwildcard"
      |> add_node "a/+/c/+/e/+" "abcdef"
      |> add_node "asdfies" "QWERTIES"
    in
    let r =
      [
        "helo/pretty/wurld";
        "helo/heh/qwert/blah";
        "omg/lol/bbqz";
        "helo/pretty";
        "omg/wtf";
        "omg/wtf!";
        "a/lol/c/def/e/orly";
        "alol/c/def/e";
      ]
    in
    let expected =
      [
        [ "helothere" ];
        [];
        [ "omgwildbbq" ];
        [];
        [ "omgwildcard"; "omgwtf" ];
        [ "omgwildcard" ];
        [ "abcdef" ];
        [];
      ]
    in
    let printer = String.concat "," in
    let cmp a b =
      let s = List.sort (fun x y -> String.compare x y) in
      s a = s b
    in
    let ae = assert_equal ~printer ~cmp in
    let res = List.map (fun x -> query x tree) r in
    List.iter2 (fun x y -> ae x y) expected res

  let pound_test _ =
    let root =
      add_node "a/#" "a" empty
      |> add_node "a/b/#" "ab"
      |> add_node "a/b" "plainab"
      |> add_node "a/b/c/#" "abc"
      |> add_node "a/b/c/d/#" "abcd"
      |> add_node "a/b/c/d/e/#" "abcde"
    in
    let r = [ "a"; "a/b"; "a/b/c"; "a/c"; "d/e" ] in
    let expected =
      [ [ "a" ]; [ "plainab"; "ab"; "a" ]; [ "abc"; "ab"; "a" ]; [ "a" ]; [] ]
    in
    let printer = String.concat "," in
    let res = List.map (fun x -> query x root) r in
    List.iter2 (fun x y -> assert_equal ~printer x y) expected res

  let remove_test _ =
    let root =
      add_node "a" "a" empty
      |> add_node "a/+" "a+"
      |> add_node "a/#" "a#"
      |> add_node "a/#" "tst"
      |> add_node "a" "tst" (* to double check wilds *)
      |> add_node "a/b" "ab"
      |> add_node "a/b/#" "ab#"
      |> add_node "a/b" "ab2"
      |> add_node "a/+/c" "a+c"
      |> add_node "a/b/c" "abc"
      |> add_node "a/+/c/#" "a+c#"
      |> add_node "a/b/c/d" "abcd"
      |> add_node "a/b/c/d" "abcd2"
    in
    let printer = String.concat ", " in
    let cmp a b =
      let s = List.sort (fun x y -> String.compare x y) in
      s a = s b
    in
    let ae = assert_equal ~printer ~cmp in

    (* really should modify to return the fully qualified key
       of the empty node
    *)
    let rec has_empty_leaves tree =
      let check acc x = if acc then acc else has_empty_leaves x in
      if 0 = List.length (get_branches tree) && 0 = List.length (get_value tree)
      then true
      else List.fold_left check false (get_branches tree)
    in

    (* sanity check the first element *)
    let res = query "a" root in
    ae [ "tst"; "a"; "tst"; "a#" ] res;
    let root = remove_node "a/#" "tst" root in
    let res = query "a" root in
    ae [ "tst"; "a"; "a#" ] res;
    let root = remove_node "a" "tst" root in
    let res = query "a" root in
    ae [ "a"; "a#" ] res;

    (* check that there are no empty leaves *)
    let res = query "a/b" root in
    ae [ "ab"; "ab#"; "ab2"; "a#"; "a+" ] res;
    (* should produce an empty leaf; the "#" dangles *)
    let root = remove_node "a/b/#" "ab#" root in
    let res = query "a/b" root in
    ae [ "ab"; "ab2"; "a#"; "a+" ] res;
    assert_equal ~msg:"empty leaves a/b/#" false (has_empty_leaves root);

    (* misc things, such as removing all values from a parent *)
    let res = query "a/b/c" root in
    ae [ "a+c"; "abc"; "a+c#"; "a#" ] res;
    let root = remove_node "a/b/c" "abc" root in
    let res = query "a/b/c" root in
    ae [ "a+c"; "a+c#"; "a#" ] res;
    let root = remove_node "a/+/c" "a+c" root in
    let res = query "a/b/c" root in
    ae [ "a+c#"; "a#" ] res;
    let root = remove_node "a/+/c/#" "a+c#" root in
    let res = query "a/b/c" root in
    ae [ "a#" ] res;
    assert_equal ~msg:"empty leaves; none" false (has_empty_leaves root);

    (* remove a sibling value from same key *)
    let res = query "a/b/c/d" root in
    ae [ "a#"; "abcd"; "abcd2" ] res;
    let root = remove_node "a/b/c/d" "abcd" root in
    let res = query "a/b/c/d" root in
    ae [ "a#"; "abcd2" ] res;

    (* remove nonexistent value from valid key *)
    let root = remove_node "a/b/c/d" "nothere" root in
    let res = query "a/b/c/d" root in
    ae [ "a#"; "abcd2" ] res;

    (* remove nonexistent value from invalid key *)
    let root = remove_node "a/c/d/" "rlynothere" root in
    let res = query "a/c/d" root in
    ae [ "a#" ] res;
    assert_equal ~msg:"empty leaves; final" false (has_empty_leaves root);
    ()

  let tests =
    [
      "split_test" >:: split_test;
      "plus test" >:: plus_test;
      "pound_test" >:: pound_test;
      "remove_test" >:: remove_test;
    ]
end
