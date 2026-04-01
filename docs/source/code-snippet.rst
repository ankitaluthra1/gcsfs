.. code-block:: python

  # Reading from a Standard bucket
      with fsspec.open("gs://my-standard-bucket/test.txt", "r") as f:
          data = f.read()

  # Writing to a Standard bucket
      with fs.open('my-standard-bucket/data/checkpoint.pt', 'wb') as f:
          f.write(b"checkpoint data...")