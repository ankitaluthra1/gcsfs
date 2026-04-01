.. code-block:: python

  # Reading from a Standard bucket
      with fsspec.open("gs://my-standard-bucket/test.txt", "r") as f:
          data = f.read()

  # Writing to a Standard bucket
      with fs.open('my-standard-bucket/data/checkpoint.pt', 'wb') as f:
          f.write(b"checkpoint data...")

    # Data loading from GCS
    logging.info(f"[INFO] Loading {standard_dataset_path} dataset")
    ds = datasets.load_dataset("parquet",
                               data_files=f"{standard_dataset_path}/*.parquet",
                               split="train",
                               streaming=True)


    # Checkpoint load path to GCS Standard bucket
    trainer.fit(LlamaLitModel(model),
                train_loader,
                ckpt_path=checkpoint_load_path)
