
docs: mqtt-client/Mqtt_client.mli
	esy dune build @doc
	rm -rf ./docs
	esy cp -r '#{self.target_dir}/default/_doc/_html' ./docs
	cp ./etc/odoc.css ./docs/odoc.css
