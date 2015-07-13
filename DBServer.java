package kis.lab5.server;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import result.Result;


public class DBServer {


    private Connection con = null; // соединение с БД 
    private Statement stmt = null; // оператор 
    public ArrayList<Result> lst = null;
    public DBServer(String DBName, String ip, int port) throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException{
        try {
            System.out.println("подключаю драйвер");
            Class.forName("com.mysql.jdbc.Driver").newInstance();        
            String url = "jdbc:mysql://"+ip+":"+port+"/"+DBName;// устанавливаю соединение с БД  
            System.out.println("устанавливаю соединение с БД");
            con = DriverManager.getConnection(url, "root", "aker6230"); 
            stmt = con.createStatement();
        } catch (SQLException e)
        {
            System.out.println("Невозможно подключиться к базе данных!");
            System.out.println(" >> "+e.getMessage());
            System.exit(0);
            this.Disconnect();

        } 
    };
    public int addNews(String title, String text, String data, int category_id ){
        return addNews(0, title, text, data, category_id );
    };
    public int addNews(int id, String title, String text, String data, int category_id ){
        System.out.println("2. Добавление новости заданной категории ");
        String id_string;
        if (id==0) id_string = "null";
        else id_string = Integer.toString(id);
            
        String sql = "INSERT INTO `news_agency`.`news` (`news_id`, `title`, `text`, `data`, `category_id_fk`) "
                 + "VALUES ("+id_string+", '"+title+"', '"+text+"', '"+data+"', '"+ category_id+"')";
        try 
        { 
            stmt.executeUpdate(sql);
            System.out.println("Новость "+title+ " успешно добавлена!"); 
            return 0;
        } catch (SQLException e)
        {
            System.out.println("ОШИБКА! Новость "+title+ " не добавлена!");
            System.out.println(" >> "+e.getMessage());
            
            return e.getErrorCode();
        } 
    };
    public int addCategory(int id, String title ){
        System.out.println("1. Добавление новой категории" );

        String sql = "INSERT INTO `news_agency`.`category` (`category_id`, `title`) "
                 + "VALUES ("+id +", '"+title+"')";
        try 
        { 
            stmt.executeUpdate(sql);
            System.out.println("Категория "+title+ " успешно добавлена!"); 
            return 0;
        } catch (SQLException e)
        {
            System.out.println("ОШИБКА! Категория "+title+ " не добавлена!");
            System.out.println(" >> "+e.getMessage());
            return e.getErrorCode();
        } 
    };
    public int deleteNews(int id){
        System.out.println("3. Удаление новости ");

        String sql = "DELETE FROM NEWS WHERE news_id = "+id;
        try
            {
                int c = stmt.executeUpdate(sql);
                if (c>0){
                    System.out.println("Новость с идентификатором "+ id +" успешно удалена!");
                    return 0;
                } 
                else{
                    System.out.println("Новость с идентификатором "+ id +" не найдена!");
                    return 1;
                }
            } catch (SQLException e){
                System.out.println("ОШИБКА при удалении новости с идентификатором "+id);
                System.out.println(" >> "+e.getMessage());
                return 1;
            }
        };
    public synchronized ArrayList<Result> ShowNews(){
        System.out.println("4. Выдача полного списка новостей ");

        ArrayList<Result> lst = new ArrayList();
        String sql = "select * from news";
        ResultSet rs; 
        Result o;
        try {
            rs = stmt.executeQuery(sql);
            while (rs.next()) 
            { 
                o = new Result();
                int id = rs.getInt("news_id");
                String name = rs.getString("title");
                String fKey = rs.getString("category_id_fk");
                o.id = id;
                o.name = name;
                o.fKey = fKey;
                lst.add(o);
            } 
            rs.close(); 
        } catch (SQLException ex) {
            o = new Result();
            o.id = ex.getErrorCode();
            o.name = ex.getMessage();
            o.fKey = null;
            lst.add(o);
            
            return lst;
        }
        return lst;
    };
    public synchronized ArrayList<Result> ShowCategories() {
        System.out.println("5. Выдача полного списка категорий ");
        ArrayList<Result> lst = new ArrayList();
        String sql = "select * from category"; 
        ResultSet rs; 
        Result o;
        try {
            rs = stmt.executeQuery(sql);
       
        while (rs.next()) 
        { 
            o = new Result();
            int id = rs.getInt("category_id");
            String name = rs.getString("title");
            o.id = id;
            o.name = name ;
            o.fKey = null;
            lst.add(o);
        } 
        rs.close();
         } catch (SQLException ex) {
            o = new Result();
            o.id = ex.getErrorCode();
            o.name = ex.getMessage();
            o.fKey = null;
            lst.add(o);
            
            return lst;
        }
        return lst;
    };
    private boolean Disconnect() throws SQLException{
        con.close();  // завершаю соединение
        return true;
    };
}
