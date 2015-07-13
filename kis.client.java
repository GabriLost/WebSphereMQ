package kis.lab5.client;

import com.ibm.mq.*;
import com.ibm.mq.constants.MQConstants;
import java.io.IOException;
import java.util.ArrayList;
import result.Result;

public class KISLab5Client {
    private MQQueueManager QM = null; // Менеджер
    private MQQueue Q1 = null; // Очередь запросов
    private MQQueue Q2 = null; // Очередь ответов
   // private int operation;//,id_cat,id_news;
    //private String name;
    public ArrayList <Result> lst = null;
    
    // конструктор
    public KISLab5Client(String QMName, String Q1_name, String Q2_name) throws IOException, MQException{
        // Настриваю среду
        MQEnvironment.hostname = null;
        // Устанавливаю соединение с менеджером
        QM = new MQQueueManager(QMName);
        // Открываю очередь запросов на запись
        Q1 = QM.accessQueue(Q1_name, MQConstants.MQOO_OUTPUT);
        // Открываю очередь ответов на чтение
        Q2 = QM.accessQueue(Q2_name, MQConstants.MQOO_INPUT_EXCLUSIVE);
    }

    public void addCategory(int id_cat, String name) throws IOException, MQException{
        MQMessage mq = new MQMessage();
        mq.writeInt(1); //operation = 1;
        mq.writeInt(id_cat);
        mq.writeString(name);
        Q1.put(mq);
    }
    public void addNews(int id_news, String title, String text, String date, int id_cat) throws IOException, MQException{
        MQMessage mq = new MQMessage();
        mq.writeInt(2);//operation = 2;
        mq.writeUTF(title);
        mq.writeInt(id_news);
        mq.writeUTF(text);     
        mq.writeInt(id_cat);
        mq.writeUTF(date);
        Q1.put(mq);
    }
    public void deleteNews(int id_news) throws IOException, MQException{
        MQMessage mq = new MQMessage();

        mq.writeInt(3); //operation = 3;
        mq.writeInt(id_news);
        Q1.put(mq);
    }
    public void showNews() throws IOException, MQException{
        MQMessage mq = new MQMessage();
        
        mq.writeInt(5);//operation = 5;
        Q1.put(mq);
    }
    public void showCategories() throws IOException, MQException{
        MQMessage mq = new MQMessage();
        mq.writeInt(4); //operation = 4;
        Q1.put(mq);
    }
    // Получить ответ от сервера
    public boolean printResult(){
        try{
    // Читаю сообщение из очереди ответов
                MQMessage response = new MQMessage();
                Q2.get(response);
                int resp = response.readInt();
                switch (resp){
                    case 0:
                    case 1:    
                    case 2:    
                    case 3:    
                        System.out.println("Ответ от сервера: " + resp);  
                        break;
                    case 4:
                        lst = (ArrayList<Result>)response.readObject();
                        System.out.println("id  " + "Name");
                        for ( int i=0; i<lst.size(); i++){
                            System.out.println(lst.get(i).id  +"  "+ lst.get(i).name);
                        }
                        break;
                    case 5:
                        lst = (ArrayList<Result>) response.readObject();
                        System.out.println("id  " + "Name");
                        for ( int i=0; i<lst.size(); i++){
                            System.out.println(lst.get(i).id  +"  "+ lst.get(i).name);
                        }
                        break;
                    default:
                        System.out.println("Ошибка  "+  resp);   
                }
        }catch(Exception e){  
            return false;  
        }  
         return true;
    }
    // отсоединиться
    public void disconnect() throws IOException, MQException
        {   
            Q1.close();
            Q2.close(); 
            QM.disconnect();   
        }


    public static void main(String[] args) throws IOException, MQException{  
        try{
            KISLab5Client client = new KISLab5Client("MQ","QQ","QA");
           // client.addCategory(774, "something new");
           // client.showCategories(); 
           // client.deleteNews(33);
           //client.addNews(33, "что-то", "с чем-то", "2014-03-05",4);
           // client.showNews();
            while(client.printResult()); 
            client.disconnect();
        }
        catch(Exception ex){             
            ex.printStackTrace();
        }    
    }
}
