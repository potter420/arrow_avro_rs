BUILD_DIR:=$(shell pwd)

python-package:
	docker run -ti --rm -v $(BUILD_DIR):/io -w /io/arrow_avro_rs_python konstin2/maturin build --release