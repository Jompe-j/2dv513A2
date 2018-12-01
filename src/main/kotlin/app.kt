import com.beust.klaxon.Klaxon
import java.io.File
import java.sql.*

fun main(args: Array<String>) {

    getJson()
    MyPostgres().connectToDb()

}

fun getJson() {
    val fileName = "c:/users/johna/kod/2dv513/data/testread.txt"
    val file = File(fileName)
    file.forEachLine {
        val preProcessed = Klaxon().parse<PreProcessedComment>(it)
        val comment = ProcessComment(preProcessed!!)
        println("ID: ${comment.id}    ${comment.date}")


    }
}

class PreProcessedComment(
    val id: String, val parent_id: String, val link_id: String, val name: String, val author: String,
    val body: String, val subreddit_id: String, val subreddit: String, val score: Int, val created_utc: String
)



class ProcessComment(private val comment: PreProcessedComment){
    var date: Date? = null
    var id: Long? = null
    var parent_id: Long? = null
    var link_id: Long? = null
    var subreddit_id: Long? = null

    init {
        date = processDate()
        id = processBas36(comment.id)
        parent_id = processBas36(comment.id)
        link_id = processBas36(comment.id)
        subreddit_id = processBas36(comment.id)

    }

    private fun processBas36(input: String): Long {
        return input.toLong(36)
}

    private fun processDate(): Date {
        println(comment.created_utc)
        val epochTime = comment.created_utc.toLong() * 1000
        return Date(epochTime)
    }

}

class MyPostgres {
    private val url = "jdbc:postgresql://localhost:5432/postgres"
    private val username = "postgres"
    private val password = "KLBLKX21j!"
    fun connectToDb(){

        try {
            val db = DriverManager.getConnection(url, username, password)
            println("Connected to DB")
        } catch (error: SQLException) {
            println(error.message)

        }
    }
}
