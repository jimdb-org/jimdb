 
# Prepare 语句

Prepare简介
  
1、使用PreparedStatement 命令将带 “?” 参数占位符的 SQL 语句发送到数据库返回statementId。

2、具体执行 SQL 时，客户端使用之前返回的statementId，并带上请求参数发起 PreparedExecute 执行 执行具体SQL。

3、最后使用stmtclose关闭对应Prepared Statement

4、Prepare 协议支持
   
       Binary 协议 使用 STMT_PREPARE，STMT_EXECUTE，STMT_CLOSE 目前通过 Binary 协议获取返回
   
       文本协议（目前暂不支持）


5、使用方式目前支持jdbc使用，不支持客户端命令行方式
  
支持命令类型：

      STMT_PREPARE、STMT_EXECUTE、STMT_CLOSE
   
不支持命令：

      STMT_RESET、STMT_FETCH、STMT_SEND_LONG_DATA
  
 
  
``` 
demo：
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @version V1.0
 */
public class PreparedStatementDemo {
  public static void main(String[] args)
  {
    Connection connection = null;
    PreparedStatement pstmt = null;
    String url = "jdbc:mysql://localhost:3306/test?user=root&password=root&useServerPrepStmts=false&cachePrepStmts=false";
    String sql = "SELECT name, age FROM student WHERE id = ?";

    try {
      Class.forName("com.mysql.jdbc.Driver");

      connection = DriverManager.getConnection(url);
      pstmt = connection.prepareStatement(sql);
      pstmt.setInt(1, 87);
      ResultSet resultSet = pstmt.executeQuery();
      while (resultSet.next()) {
        String name = resultSet.getString(1);
        int age = resultSet.getInt(2);
        System.out.println("Student name : " + name + " age : " + age);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        pstmt.close();
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
```
  
  
  



