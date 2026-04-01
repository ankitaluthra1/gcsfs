.. code-block:: python

  # Reading from a Rapid bucket
      with fsspec.open("gs://my-rapid-bucket/test.txt", "r") as f:
          data = f.read()

  # Writing to a Rapid bucket
      with fs.open('my-rapid-bucket/data/checkpoint.pt', 'wb') as f:
          f.write(b"checkpoint data...")

    # Data loading from GCS
    logging.info(f"[INFO] Loading {rapid_dataset_path} dataset")
    ds = datasets.load_dataset("parquet",
                               data_files=f"{rapid_dataset_path}/*.parquet",
                               split="train",
                               streaming=True)


    # Checkpoint load path to GCS Rapid bucket
    trainer.fit(LlamaLitModel(model),
                train_loader,
                ckpt_path=rapid_load_path)