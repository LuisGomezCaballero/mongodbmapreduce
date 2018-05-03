package com.luisgomezcaballero.mongodbmapreduce;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;

@SpringBootApplication
public class MongodbmapreduceApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(MongodbmapreduceApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		MongoClient mongo = null;
		try {
			mongo = new MongoClient("localhost", 27017);

			DB db = mongo.getDB("test");

			DBCollection collection = db.getCollection("myEntity");

			String map = "function () { emit(this.user_name, 1); }";

			String reduce = "function (key, values) { return Array.sum(values); }";

			DBObject dbObject = new BasicDBObject();
			dbObject.put("status", "active");

			MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce, "total",
					MapReduceCommand.OutputType.REPLACE, dbObject);

			MapReduceOutput out = collection.mapReduce(cmd);

			for (DBObject o : out.results()) {
				System.out.println(o.toString());
			}

			System.out.println("Done");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			mongo.close();
		}
	}
}
