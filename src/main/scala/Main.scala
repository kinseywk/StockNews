import java.io.File
import java.io.PrintWriter
import scala.io.StdIn.readLine
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.json4s.NoTypeHints
import org.joda.time.Days
import com.github.nscala_time.time.Imports._

case class UserDatabase(users: List[UserRecord])
case class UserRecord(username: String, password: String)

case class NewsApiResponse(
  status: String,
  totalResults: Int,
  articles: List[NewsApiArticle]
)
case class NewsApiArticle(
  source: NewsApiSource,
  author: String,
  title: String,
  description: String,
  url: String,
  urlToImage: Option[String],
  publishedAt: String,
  content: String
)

case class NewsApiSource(id: Option[String], name: String)

case class CompanyRecord(name: String, stockSymbol: String, history: List[DayRecord])
case class DayRecord(date: DateTime, stockOpen: Float, stockClose: Float, articles: List[ArticleRecord])
case class ArticleRecord(datePublished: DateTime, headline: String, headlineSentiment: Float, summary: String, sample: String)
  
object Main extends App {
  val VERSION = "1.0"
  val DATA_DIRECTORY = "data/"
  val USERS_JSON_URL = DATA_DIRECTORY + "users.json"
  val COMPANIES_JSON_URL = DATA_DIRECTORY + "companies.json"
  val ADMIN_USERNAME = "kyle"
  val SCRAPE_TIMESPAN = 3.days
  val NEWSAPI_KEY = "c9cfa14c38a242f8aa7290569e680b27"
  val NEWSAPI_MAX_PAGES = 3
    
  implicit val formats = Serialization.formats(NoTypeHints)

  //Returns 0 if login failed, 1 if basic login, 2 if admin
  def attemptLogin(username: String, password: String): Int = {
    var result = 0
    val userDb = read[UserDatabase](scala.io.Source.fromFile(USERS_JSON_URL).mkString(""))
    
    userDb.users.foreach((x) => {
      if(x.username == username && x.password == password) {
        if(username == ADMIN_USERNAME) {
          result = 2
        } else {
          result = 1
        }
      }
    })

    result
  }

  //Returns 0 if signup failed, 1 if basic login, 2 if admin
  def attemptSignup(username: String, password: String): Int = {
    var result = 0
    var found = false
    var userDb = read[UserDatabase](scala.io.Source.fromFile(USERS_JSON_URL).mkString(""))

    userDb.users.foreach((x) => {
      if(x.username == username) {
        found = true
      }
    })

    if(!found) {
      val newUserDb = UserDatabase(userDb.users :+ (UserRecord(username, password)))
      val newUserDbJson = write[UserDatabase](newUserDb)
      val writer = new PrintWriter(new File(USERS_JSON_URL))
      writer.write(newUserDbJson)
      writer.close()

      if(username == ADMIN_USERNAME) {
        result = 2
      } else {
        result = 1
      }
    }

    result
  }
  
  def scrapeHeadlines(company: String): List[DayRecord] = {
    //NewsAPI free plan allows you to pull headlines from up to a month ago, so loop through each day since then and pull headlines
    var result = List[DayRecord]()
    var cursor = DateTime.now()
    val endDate = cursor - SCRAPE_TIMESPAN
    var resultsCount = 0
    var resultsPages = 0
    var totalResults = 0

    //Loop per day
    do {
      var dailyNewsApiArticles = List[NewsApiArticle]()
      var dailyArticleRecords = List[ArticleRecord]()

      //Loop per results page (up to limit)
      do {
        val year: Int = cursor.year().get()
        val month: Int = cursor.monthOfYear().get()
        val day: Int = cursor.dayOfMonth().get()
        val requestUrl = s"https://newsapi.org/v2/everything?q=${company}&from=${year}-${month}-${day}&to=${year}-${month}-${day}&sortBy=publishedAt&apiKey=${NEWSAPI_KEY}"
        val responseJson = scala.io.Source.fromURL(requestUrl).mkString("")
        var responseObject = read[NewsApiResponse](responseJson)
        totalResults = responseObject.totalResults
        dailyNewsApiArticles = dailyNewsApiArticles ::: responseObject.articles
        dailyNewsApiArticles.foreach(x => dailyArticleRecords = dailyArticleRecords :+ ArticleRecord(cursor, x.title, 0.0f, x.description, x.content))
        dailyArticleRecords.foreach(x => println(s" > ${x.headline}"))
        
        resultsCount += responseObject.articles.length
        resultsPages += 1
        
        println(s"\nScraping news headlines for company ${company}: Received results page #${resultsPages} for ${month}/${day}/${year}, found articles ${resultsCount - responseObject.articles.length + 1} through ${resultsCount}...")
        //Loop through pages until reading all results or reaching page limit
      } while(resultsCount < totalResults && resultsPages < NEWSAPI_MAX_PAGES)

      result = result :+ DayRecord(cursor, 0.0f, 0.0f, dailyArticleRecords)
      cursor = cursor - 1.days
    } while(Math.abs(Days.daysBetween(cursor, endDate).getDays) > 0)

    println(s"Job complete!\n\nScraped ${resultsCount} of ${totalResults} headlines pertaining to company ${company}.\n\nPress Enter to continue.")
    readLine()

    result
  }

  //Scrape past month of headlines for a company and pull their daily stock performance stock quote
  def scrape(company: String, stockSymbol: String) {
    println("\u001b[2J")

    var history = scrapeHeadlines(company)
    /*
    history = nlpHeadlines(history)
    history = scrapeStockQuotes(company, stockSymbol, history)
    history = calculateCorrelation(history)
    */

    var companyRecord = CompanyRecord(company, stockSymbol, history)
    
    val writer = new PrintWriter(new File(s"${DATA_DIRECTORY}${company}.json"))
    writer.write(write(companyRecord))
    writer.close()

    //TODO: Also insert into companies.json

    println()
  }

  def nlpHeadlines(history: List[DayRecord]): List[DayRecord] = {
    var result = List[DayRecord]()
    result
  }

  def scrapeStockQuotes(company: String, stockSymbol: String, history: List[DayRecord]): List[DayRecord] = {
    var result = List[DayRecord]()
    result
  }

  def calculateCorrelation(history: List[DayRecord]): List[DayRecord] = {
    var result = List[DayRecord]()
    result
  }

  def loadCompanyRecord(company: String) {

  }

  def loadCompanyDatabase() {

  }

  val LOGIN_MENU = "Login Menu\n 1: Login\n 2: Signup\n 3: Quit"
  val MAIN_MENU_BASIC = "Main Menu\n 1: View Company Data\n 2: Quit"
  val MAIN_MENU_ADMIN = "Main Menu\n 1: Scrape Company Data\n 2: Load Company Data\n 3: View Company Data\n 4: Quit"
  val VIEW_COMPANY_MENU = "Press Enter to return to previous menu"

  /*States:
   -1: Done, quitting
    0: Login Menu (1 = "Login", 2 = "Signup", 3 = "Quit")
    1: Main Menu as basic user (1 = "View Company Data", 2 = "Quit")
    2: Main Menu as admin user (1 = "Scrape Company Data", 2 = "Load Company Data", 3 = "View Company Data", 4 = "Quit")
    3: View Company Menu (* = "Return")
  */
  var state = 0
  var menu = LOGIN_MENU

  println(s"\u001b[2Jstock-news Headline Sentiment-to-Stock Performance Correlation Calculator v${VERSION}\n\n")

  do {
    print(s"${menu + "\n\nOption: "}")

    state match {
      //Login Menu
      case 0 => {
        readLine() match {
          case "1" => {
            print("Username: ")
            val username = readLine()
            print("Password: ")
            val password = readLine()

            attemptLogin(username, password) match {
              case 0 => println("\u001b[2JLogin failed. Bad credentials, try again.\n")
              case 1 => {
                state = 1
                menu = MAIN_MENU_BASIC
                println(s"\u001b[2JLogin success! Welcome, basic user ${username}\n")
              }
              case 2 => {
                state = 2
                menu = MAIN_MENU_ADMIN
                println(s"\u001b[2JLogin success! Welcome, admin user ${username}\n")
              }
            }
          }
          case "2" => {
            print("Username: ")
            val username = readLine()
            print("Password: ")
            val password = readLine()
            
            attemptSignup(username, password) match {
              case 0 => {
                println("\u001b[2JSignup failed. Username already taken, try again.\n")
                state = 0
                menu = LOGIN_MENU
              }
              case 1 => {
                println(s"\u001b[2JSignup complete. Welcome, user ${username}! Returning to main menu.\n")
                state = 1
                menu = MAIN_MENU_BASIC
              }
              case 2 => {
                println(s"\u001b[2JSignup complete. Welcome, administrator ${username}! Returning to main menu.\n")
                state = 2
                menu = MAIN_MENU_ADMIN
              }
            }
          }
          case "3" => state = -1
        }
      }
      //Basic Main Menu
      case 1 => {
        readLine() match {
          case "1" => {
            state = 3
            menu = VIEW_COMPANY_MENU
          }
          case "2" => state = -1
        }
      }
      //Admin Main Menu
      case 2 => {
        readLine() match {
          //Scrape company data from API endpoints and save JSON to disk
          case "1" => {
            print("Company: ")
            val company = readLine()
            print("Stock Symbol: ")
            val stock = readLine()
            scrape(company, stock)
          }
          //Load company data from disk into Hive/Spark
          case "2" => {
            state = -1

          }
          case "3" => {
            state = 3
            menu = VIEW_COMPANY_MENU
          }
          case "4" => state = -1
        }
      }
      //Company Menu
      case 3 => {
        readLine()
      }
    }
  } while(state != -1)
}