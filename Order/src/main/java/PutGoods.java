import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class PutGoods{
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    public static void main(String[] args)throws IOException{
        init();
        putGoods("goods");
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
    public static void putGoods(String tableName) throws IOException {
        FileReader reader = new FileReader("/headless/Desktop/workspace/hbase_pro/Order/Data/goods.dat");
        BufferedReader br = new BufferedReader(reader);
        String str = null;
        Table table = connection.getTable(TableName.valueOf(tableName));
        while((str = br.readLine()) != null) {
            System.out.println("processing line:" + str);
            String[] cols = str.split("\t");
            Put put = new Put(cols[2].getBytes());
            put.addColumn("MID".getBytes(), "".getBytes(), cols[0].getBytes());
            put.addColumn("T_name".getBytes(), "".getBytes(), cols[1].getBytes());
            put.addColumn("TID".getBytes(), "".getBytes(), cols[2].getBytes());
            put.addColumn("Price".getBytes(), "".getBytes(), cols[3].getBytes());
            put.addColumn("Yield".getBytes(), "".getBytes(), cols[4].getBytes());
            put.addColumn("Sales".getBytes(), "".getBytes(), cols[5].getBytes());
            table.put(put);
        }
        table.close();
        br.close();
        reader.close();
    }
}