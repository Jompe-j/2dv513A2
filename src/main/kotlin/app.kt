
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.sql.*
import com.fasterxml.jackson.module.kotlin.*
import java.math.BigInteger

fun main(args: Array<String>) {

    getJson()

}

fun getJson() {
    val fileName = "c:/users/johna/kod/2dv513/data/RC_2007-10.json"
    val file = File(fileName)

    var br = BufferedReader(FileReader(file))
    var counter = 1
    val mutableList = mutableListOf<String>()
    var line: String? = br.readLine()
    while (line != null) {
        mutableList.add(line)
        counter++
        if (counter > 1000000) {


            MyPostgres().batchInsert(processList(mutableList))
            mutableList.clear()
            counter = 0
            println("New batch")
        }

        line = br.readLine()
        if (line == null && mutableList.lastIndex != -1) {
            MyPostgres().batchInsert(processList(mutableList))
            mutableList.clear()
        }

    }
}

fun processList(mutableList: MutableList<String>): MutableList<ProcessComment> {
    val objectList = mutableListOf<ProcessComment>()
    val mapper = jacksonObjectMapper()
    var counter = 0
    val startTime = System.currentTimeMillis()
    for (s in mutableList) {
        val preProcessed = mapper.readValue<PreProcessedComment>(s)
        val processed = ProcessComment(preProcessed)
        objectList.add(processed)
        counter++
    }
    val stoptime = System.currentTimeMillis()
    println(counter)
    println("Time: ${stoptime - startTime}")
    println("Batch done")
    return objectList
}

class PreProcessedComment(
    val id: String, val parent_id: String, val link_id: String, val name: String, val author: String,
    val body: String, val subreddit_id: String, val subreddit: String, val score: Int, val created_utc: String,
    val ups: String?, val controversiality: Int?, val distinguished: String?, val downs: Int?, val archived: String?,
    val score_hidden: String?, val gilded: Int?, val author_flair_text: String?, val edited: String?,
    val author_flair_css_class: String?, val retrieved_on: Int?
)

class ProcessComment(private val comment: PreProcessedComment){
    var id: BigInteger? = null
    var parent_id = comment.parent_id
    var link_id = comment.link_id
    var name = comment.name
    val author = comment.author
    val body = comment.body
    val subreddit_id = comment.subreddit_id
    val subreddit = comment.subreddit
    val score = comment.score
    var date: Date? = null

    init {
        date = processDate()
        id = processBas36(comment.id)
        //parent_id = processBas36(comment.parent_id.removeRange(0, 3))
        //link_id = processBas36(comment.link_id)
        // subreddit_id = processBas36(comment.subreddit_id)

    }

    private fun processBas36(input: String): BigInteger {
        return input.toBigInteger(36)
    }

    private fun processDate(): Date {
        val epochTime = comment.created_utc.toLong() * 1000
        return Date(epochTime)
    }

}

class MyPostgres {
    private val url = "jdbc:postgresql://localhost:5432/postgres"
    private val username = "postgres"
    private val password =
    fun connectToDb(): Connection? {
        var db: Connection? = null

        try {
            db = DriverManager.getConnection(url, username, password)
            println("Connected to DB")

        } catch (error: SQLException) {
            println(error.message)

        }
        return db
    }

    fun batchInsert(objectList: MutableList<ProcessComment>){

        val db = connectToDb()
        db?.autoCommit = false
        val startTime = System.currentTimeMillis()
        var query = "INSERT INTO redditcomment (parent_id, link_id, name, author, body, subreddit_id, subreddit, score, created_utc, id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        var pstmt = db?.prepareStatement(query)
        for(comment in objectList){
            pstmt?.setString(1, comment.parent_id)
            pstmt?.setString(2, comment.link_id)
            pstmt?.setString(3, comment.name)
            pstmt?.setString(4, comment.author)
            pstmt?.setString(5, comment.body)
            pstmt?.setString(6, comment.subreddit_id)
            pstmt?.setString(7, comment.subreddit)
            pstmt?.setInt(8, comment.score)
            pstmt?.setDate(9, comment.date)
            pstmt?.setObject(10, comment.id)
            pstmt?.addBatch()

        }
        pstmt?.executeBatch()
        db?.commit()

        db?.close()
        if(db!!.isClosed){
            println("Db is closed")
            val stoptime = System.currentTimeMillis()
            println("-----------------------Insert time Time: ${stoptime - startTime}")
            println("Insert done")
        }

    }

}





/*
val query = "INSERT INTO redditcomment values ('ParentID', 'LinkD', 'MyName', 'Author', 'This is a comment', 'SUBRID', 'SUBR', 13, to_date('2007-10-12', 'YYYY-MM-DD'),  100000)"
var st: Statement = db.createStatement()
st.executeUpdate(query)*/
