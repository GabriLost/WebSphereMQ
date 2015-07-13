package kis.lab5.server;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import com.ibm.mq.*;
import com.ibm.mq.constants.MQConstants;
import java.rmi.RemoteException;
import java.util.ArrayList;
import result.Result;

public class KISLab5Server {
    private MQQueueManager QM = null; // Менеджер очередей
    private MQQueue Q1 = null; // Очередь запросов
    private MQQueue Q2 = null; // Очередь ответов
    private MQMessage resp = null;
    public DBServer DB = null;
    public int answ;
    public ArrayList<Result> lst = null;
    public Result nw = null;
    // запустить сервер
    public void start(String QMName, String Q1_name, String Q2_name) throws MQException, SQLException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        DB = new DBServer("news_agency","localhost",3306);
        MQEnvironment.hostname = null;
// Устанавливаю соединение с менеджером
        QM = new MQQueueManager(QMName);
// Открываю очередь запросов на чтение
        Q1 = QM.accessQueue(Q1_name,MQConstants.MQOO_INPUT_SHARED);
// Открываю очередь ответов на запись
        Q2 = QM.accessQueue(Q2_name,MQConstants.MQOO_OUTPUT);
        resp = new MQMessage();
// Обрабатываю все запросы
        int i=0;
        while (processQuery()) i++;
// Завершаю работу
        Q1.close(); 
        Q2.close(); 
        QM.disconnect();
        System.out.println("Обработано "+i+" запросов");
 }
    // Обработка сообщения-запроса
    public boolean processQuery() throws IOException, MQException, SQLException{
        int id_news, id_cat;
        String name, title, text, date;
        try{
            // Настраиваю интервал ожидания 3 сек.
            MQGetMessageOptions gmo =   new MQGetMessageOptions();
            gmo.options = MQConstants.MQGMO_WAIT;
            gmo.waitInterval = 3000;
            // Читаю сообщение из очереди запросов
            MQMessage query = new MQMessage();
            Q1.get(query,gmo);        
            // Обрабатываю сообщение
            int oper = query.readInt(); // Операция
            switch (oper){
                case 1:
                    id_cat = query.readInt();
                    name = query.readLine();
                    addCat(id_cat, name);
                    break; 
                case 2:
                    title = query.readUTF();
                    id_news = query.readInt();
                    text =  query.readUTF();
                    id_cat = query.readInt();
                    date = query.readUTF();
                    addNews(id_news, title, text, date, id_cat);
                    break;
                case 3:
                    id_news = query.readInt();
                    deleteNews(id_news);
                    break;
                case 4:
                    showNews();
                    break;
                case 5:
                    showCategories();
                    break;
                default:
                    answ = oper;
                    resp.writeInt(answ);
                    Q2.put(resp);
                    return false;
            }
            return true;
        }
        catch(IOException ex)
            {
                System.out.println("IOException");
                ex.printStackTrace();
                answ = 0;
                resp.writeInt(answ);
                Q2.put(resp);
                return false;    
            }
        catch (MQException exc)   
            {
                exc.getMessage();
                System.out.println("Очередь пуста");
                return false;    
            }
    }
    
    public int addCat(int id, String title) throws IOException, MQException{
        System.out.println("addCat");
        resp = new MQMessage();
        answ = DB.addCategory(id, title);
        resp.writeInt(answ);
        Q2.put(resp);
        return answ;
    }
    public int addNews(int id, String title, String text, String date, int id_cat) throws SQLException, IOException, MQException {            
        System.out.println("addNews");
        resp = new MQMessage();
        answ = DB.addNews(id, title, text, date, id_cat);
        resp.writeInt(answ);
        Q2.put(resp);
        return answ;
    }
    public int deleteNews(int id) throws SQLException, IOException, MQException{
        System.out.println("deleteNews");
        resp = new MQMessage();
        answ = DB.deleteNews(id);
        resp.writeInt(answ);
        Q2.put(resp);
        return answ;
    }
    public void showCategories() throws IOException, MQException {
        System.out.println("showCategories");
        lst = DB.ShowCategories();
        resp = new MQMessage();
        if (lst != null)
        {
            resp.writeInt(4); //answ= 4
            resp.writeObject(lst);
            Q2.put(resp);
        }
        else
        {
            resp.writeInt(0);//answ= 0
            Q2.put(resp);
        }
    }
    public void showNews() throws IOException, MQException{   
        System.out.println("showNews");
        resp = new MQMessage();
        lst = DB.ShowNews();
        if (lst != null)
        {
            resp.writeInt(5); //answ= 5
            resp.writeObject(lst);
            Q2.put(resp);
        }
            else
        {
            resp.writeInt(0);//answ = 0;
            Q2.put(resp);
        }
    }
    public static void main(String[] args) throws MQException, SQLException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException{
        (new KISLab5Server()).start("MQ","QQ","QA");
    }
}
