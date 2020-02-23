package databasetest;

import java.sql.*;
import java.util.ArrayList;
import org.json.simple.*;


public class DatabaseTest {
    
   public static void main(String[] args) {
        
        JSONArray array = (JSONArray) getJSONData();
        System.out.println("\nCONVERSION RESULTS (DATABASE TO JSON)");
        System.out.println("=====================================");
        System.out.println(array);
        System.out.println();
        
    }

    public static JSONArray getJSONData() {
                
        Connection connection = null;
        PreparedStatement pstmt = null, pstUpdate = null;
        ResultSet resultset = null;
        ResultSetMetaData metadata = null;
        
        JSONArray list = new JSONArray();
        
        String query, key, value;
      
        ArrayList<String> records = new ArrayList<>();
        
        
        boolean hasresults;
        int resultCount, columnCount, updateCount = 0;
        
        try {
            
            /* Identify the Server */
            
            String server = ("jdbc:mysql://localhost/p2_test");
            String username = "root";
            String password = "CS488";

            /* Load the MySQL JDBC Driver */
            
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            
            /* Open Connection */

            connection = DriverManager.getConnection(server, username, password);

            /* Test Connection */
            
            if (connection.isValid(0)) {
                
                /* Connection Open! */
                
                System.out.println("Connected Successfully!");
  
                /* Prepare Select Query */
                
                query = "SELECT * FROM people";
                pstmt = connection.prepareStatement(query);
                
                /* Execute Select Query */
                
                hasresults = pstmt.execute();                
                
                /* Get Results */
                
                System.out.println("Getting Results ...");
                
                while ( hasresults || pstmt.getUpdateCount() != -1 ) {

                    if ( hasresults ) {
                        
                        /* Get ResultSet Metadata */
                        
                        resultset = pstmt.getResultSet();
                        metadata = resultset.getMetaData();
                        columnCount = metadata.getColumnCount();
                        
                        /* Get Column Names; Append them in an ArraList "key" */
                        
                        for (int i = 2; i <= columnCount; i++) {
                            records.add(metadata.getColumnLabel(i));
                        }
                        
                        /* Get Data; Put the data in JSONObject */
                        
                        while(resultset.next()) {
                            
                            /* Begin Next ResultSet Row; Loop Through ResultSet 
                            Columns; Append to jsonObject */
                            
                            JSONObject object = new JSONObject();

                            for (int i = 2; i <= columnCount; i++) {
                                
                                JSONObject jsonObject = new JSONObject();
                                value = resultset.getString(i);
                                
                                if (resultset.wasNull()) {
                                    jsonObject.put(records.get(i-2), "NULL");
                                    jsonObject.toJSONString();
                                }

                                else {
                                    jsonObject.put(records.get(i-2), value);
                                    jsonObject.toString();
                                }
                                
                                object.putAll(jsonObject);

                            }
                            list.add(object);

                        }
                        
                    }

                    else {

                        resultCount = pstmt.getUpdateCount();  

                        if ( resultCount == -1 ) {
                            break;
                        }

                    }
                    
                    /* Check for More Data */

                    hasresults = pstmt.getMoreResults();

                }
                
            }
            
            /* Close Database Connection */
            
            connection.close();
            
        }
        
        catch (Exception e) {
            System.err.println(e.toString());
        }
        
        /* Close Other Database Objects */
        
        finally {
            
            if (resultset != null) { try { resultset.close(); resultset = null; } catch (Exception e) {} }
            
            if (pstmt != null) { try { pstmt.close(); pstmt = null; } catch (Exception e) {} }
                       
        }
        return list;
    }
    
}