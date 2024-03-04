import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class PutOrder{
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    public static void main(String[] args)throws IOException{
        init();
        putOrder("order");
        close();
    }

    public static void init(){

        configuration  = HBaseConfiguration.create();
        try{

            connection = ConnectionFactory.createConnection(configuration);

            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void putOrder(String tableName) throws IOException {
        FileReader reader = new FileReader("/headless/Desktop/workspace/hbase_pro/Order/Data/order.dat");
        BufferedReader br = new BufferedReader(reader);
        String str = null;
        Table table = connection.getTable(TableName.valueOf(tableName));
        while((str = br.readLine()) != null) {
            System.out.println("processing line:" + str);
            String[] cols = str.split("\t");
            Put put = new Put(cols[5].getBytes());
            put.addColumn("M_name".getBytes(), "".getBytes(), cols[0].getBytes());
            put.addColumn("MID".getBytes(), "".getBytes(), cols[1].getBytes());
            put.addColumn("T_name".getBytes(), "".getBytes(), cols[2].getBytes());
            put.addColumn("TID".getBytes(), "".getBytes(), cols[3].getBytes());
            put.addColumn("num".getBytes(), "".getBytes(), cols[4].getBytes());
            put.addColumn("OID".getBytes(), "".getBytes(), cols[5].getBytes());
            put.addColumn("Order_time".getBytes(), "".getBytes(), cols[6].getBytes());
            put.addColumn("Location".getBytes(), "".getBytes(), cols[7].getBytes());
            put.addColumn("Money".getBytes(), "".getBytes(), cols[8].getBytes());
            put.addColumn("Discount".getBytes(), "".getBytes(), cols[9].getBytes());
            table.put(put);
        }
        table.close();
        br.close();
        reader.close();
    }
}