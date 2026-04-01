.. code-block:: python

  # Reading from a Rapid bucket
      with fsspec.open("gs://my-rapid-bucket/test.txt", "r") as f:
          data = f.read()

  # Writing to a Rapid bucket
      with fs.open('my-rapid-bucket/data/checkpoint.pt', 'wb') as f:
          f.write(b"checkpoint data...")