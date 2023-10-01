# Assignment Day 18 Dibimbing Data Engineering Batch 2

## Exercise
- Make event producer that
  - Produce purchasing event (covered)
- Make Streaming Job that
  - Listen to kafka topic
  - Aggregate for daily total purchase (the triggers is just like the examples)
  - Write it to console with these columns
    - Timestamp
    - Running total
Hint:
Use foreachbatch or complete mode

## Apa saja yang dilakukan
1. Mendefinisikan path menuju folder checkpoint untuk kebutuhan stateful transformation.
   ![folder](https://github.com/ferrysetefanus/D18DE2_Ferry-Setefanus/blob/main/img/checkpoin_folder.jpg)
   
2. Mendefinisikan path menuju folder pada script.
   ![path script](https://github.com/ferrysetefanus/D18DE2_Ferry-Setefanus/blob/main/img/checkpoint_path.jpg)
      
3. Mengubah script kafka consumer agar bisa melakukan transformasi menggunakan watermarking dengan batas 1 hari dan output type complete untuk menghitung running total
   yang merupakan total_sum dari setiap batch yang diproses dan ditampilkan dalam bentuk konsol
   ```
    # parsing json yang dihasilkan producer menjadi string untuk mempermudah data manipulation
    parsed_stream_df = stream_df.selectExpr("CAST(value AS STRING)").selectExpr("from_json(value, 'ts TIMESTAMP, price INT') as data").select("data.*")

    # membuat batas waktu maksimal data terlambat yang masih bisa diproses, karena casenya daily total purchase maka batas maksimum nya adalah 1 hari
    windowed_stream_df = parsed_stream_df.withWatermark("ts", "1 day").groupBy(window("ts", "1 day")).agg(expr("sum(price) as total_purchase"))

    # Menampilkan kolom "Timestamp" and "Running total" dengan tipe output complete mode agar semua agregasi pada setiap batch bisa dijumlahkan
    (
        windowed_stream_df
        .selectExpr("window.start as Timestamp", "total_purchase as Running_total")
        .writeStream
        .outputMode("complete") 
        .format("console")
        .option("checkpointLocation", checkpoint_path)
        .start()
        .awaitTermination()
    )


   ```
   
4. Hasil yang ditampilkan pada console
   ![console](https://github.com/ferrysetefanus/D18DE2_Ferry-Setefanus/blob/main/img/console.jpg)
   
# Cara Menjalankan
make spark >> make kafka >> make spark-produce >> make spark-consume


