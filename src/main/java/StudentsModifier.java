import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.ArrayList;

/**
 * Created by leandro.mora<leandro.jorge.mora@gmail.com>
 * The assignment of this POC consists of removing the lowest score homework for each student from a school collection.
 * The documents have the following structure:
 * {
     "_id" : 0,
     "name" : "aimee Zank",
     "scores" : [
         {
             "type" : "exam",
             "score" : 1.463179736705023
         },
         {
             "type" : "quiz",
             "score" : 11.78273309957772
         },
         {
             "type" : "homework",
             "score" : 6.676176060654615
         },
         {
             "type" : "homework",
             "score" : 35.8740349954354
         }
     ]
 *}
*
* You can import the collection from the "students.json" document located in the resource folder. Please type:
* ./mongoimport -d school -c students < /home/leomora/Projects/mongoUniversity/homework/week3/students.json
 *
 */
public class StudentsModifier {


    /**
     * The performance is not good because you extract-process-put again an object in the database.
     * @param mongoURIString
     * @return
     * @throws UnknownHostException
     */
    public boolean removeWorstStudentHomework(String mongoURIString) throws UnknownHostException {
        // TODO 1) Add JUNIT
        // TODO 2) It would be great to test it with MORPHIA.

        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoURIString));

        DB db = mongoClient.getDB("school");

        DBCollection students = db.getCollection("students");

        System.out.println("NUMBER OF RECORDS" + students.count());

        DBCursor studentsCursor = students.find();

        while (studentsCursor.hasNext()) {

            DBObject student = studentsCursor.next();

            removeHomework(student);

            students.update(new BasicDBObject("_id", student.get("_id")), student, false, false);

        }

        return true;
    }


    private void removeHomework(DBObject student) {

        ArrayList<DBObject> listStudentsAssigments = (ArrayList<DBObject>)student.get("scores");

        DBObject homeworkToDelete = getMinorHomeworkResult(listStudentsAssigments);


        if(homeworkToDelete != null){

            listStudentsAssigments.remove(homeworkToDelete);

            System.out.println(new StringBuilder().append("StudentsModifier.removeHomework - homework to be removed").append(listStudentsAssigments).toString());

        }

    }

    private DBObject getMinorHomeworkResult(ArrayList<DBObject> userScores) {

        DBObject homeworkToDelete = null;
        double min_value = Double.MAX_VALUE;


        for(DBObject studentAssigment: userScores){

            String type = (String)studentAssigment.get("type");

            Double score = (Double) studentAssigment.get("score");

            if(type.equals("homework") && score < min_value){
                min_value = score;
                homeworkToDelete = studentAssigment;
            }
        }



        return homeworkToDelete;
    }


    /************************************************************************************
     * MAIN METHOD
     * **********************************************************************************/

     public static void main(String[] args) throws UnknownHostException {

        String mongoURIString;

        if (args.length == 0) {
            mongoURIString = "mongodb://localhost";
        }
        else {
            mongoURIString = args[0];
        }

        StudentsModifier modifier = new StudentsModifier();
        boolean success = modifier.removeWorstStudentHomework(mongoURIString);

        if (success){
            System.out.println("Success!!");
        }
    }
}

