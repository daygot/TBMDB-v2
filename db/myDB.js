const { MongoClient, ObjectId } = require("mongodb");

function myDB() {
  const myDB = {};
  const dbName = "TBMDB";
  const colNameM = "Movies";
  const colNameP = "People";
  const uri = process.env.MONGO_URL || "mongodb+srv://dbUser:dbUserPassword@cluster0.sx2rx.mongodb.net/TBMDB";

  myDB.getMovies = async function (page) {
    const client = MongoClient(uri, { useUnifiedTopology: true });
    try {
      await client.connect();
      const db = client.db(dbName);
      const col = db.collection(colNameM);
      const query = {};

      return await col
        .find(query)
        // sort in descending order by creation
        .sort({ _id: -1 })
        .limit(20)
        .toArray();
    } finally {
      client.close();
    }
  };

  myDB.createMovie = async function (movie) {
    const client = MongoClient(uri, { useUnifiedTopology: true });
    try {
      await client.connect();
      const db = client.db(dbName);
      const col = db.collection(colNameM);

      return await col.insertOne(movie);
    } finally {
      client.close();
    }
  };

  myDB.updateMovie = async function (movie) {
    const client = MongoClient(uri, { useUnifiedTopology: true });
    try {
      await client.connect();
      const db = client.db(dbName);
      const col = db.collection(colNameM);

      return await col.updateOne(
        { _id: ObjectId(movie._id) },
        {
          $set: {
            title: movie.title,
            box_office: movie.box_office,
            release_date: movie.release_date,
            studio: movie.studio,
            user_rating: movie.user_rating,
            cast: movie.cast,
            genre: movie.genre,
            country: movie.country,
            rating: movie.rating,
            language: movie.language,
            director: movie.director
          },
        }
      );
    } finally {
      client.close();
    }
  };

  myDB.deleteMovie = async function (movie) {
    const client = MongoClient(uri, { useUnifiedTopology: true });
    try {
      await client.connect();
      const db = client.db(dbName);
      const col = db.collection(colNameM);

      return await col.deleteOne({ _id: ObjectId(movie._id) });
    } finally {
      client.close();
    }
  };

  myDB.getPeople = async function (page) {
    const client = MongoClient(uri, { useUnifiedTopology: true });
    try {
      await client.connect();
      const db = client.db(dbName);
      const col = db.collection(colNameP);
      const query = {};

      return await col
        .find(query)
        // sort in descending order by creation
        .sort({ _id: -1 })
        .limit(20)
        .toArray();
    } finally {
      client.close();
    }
  };

  myDB.createPerson = async function (person) {
    const client = MongoClient(uri, { useUnifiedTopology: true });
    try {
      await client.connect();
      const db = client.db(dbName);
      const col = db.collection(colNameP);

      return await col.insertOne(person);
    } finally {
      client.close();
    }
  };

  myDB.updatePerson = async function (person) {
    const client = MongoClient(uri, { useUnifiedTopology: true });
    try {
      await client.connect();
      const db = client.db(dbName);
      const col = db.collection(colNameP);

      return await col.updateOne(
        { _id: ObjectId(person._id) },
        {
          $set: {
            first_name: person.first_name,
            last_name: person.last_name,
            age: person.age
          },
        }
      );
    } finally {
      client.close();
    }
  };

  myDB.deletePerson = async function (person) {
    const client = MongoClient(uri, { useUnifiedTopology: true });
    try {
      await client.connect();
      const db = client.db(dbName);
      const col = db.collection(colNameP);

      return await col.deleteOne({ _id: ObjectId(person._id) });
    } finally {
      client.close();
    }
  };

  return myDB;
}

module.exports = myDB();
