import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Scanner;

public class Order_System {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void init() {
        configuration = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("init success!");
    }

    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void addGoods(String tableName) throws IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please input the Trade ID:");
        String TID = scanner.next();
        Get get = new Get(TID.getBytes());
        Table table = connection.getTable(TableName.valueOf(tableName));
        if (!table.exists(get)){
            System.out.println("Please input the merchant ID:");
            String MID = scanner.next();
            System.out.println("Please input the Trade name:");
            String T_name = scanner.next();
            System.out.println("Please input the Price:");
            String Price = scanner.next();
            System.out.println("Please input the Yield:");
            String Yield = scanner.next();
            System.out.println("Please input the Sales:");
            String Sales = scanner.next();
            Put put = new Put(TID.getBytes());
            put.addColumn("MID".getBytes(), "".getBytes(), MID.getBytes());
            put.addColumn("T_name".getBytes(), "".getBytes(), T_name.getBytes());
            put.addColumn("TID".getBytes(), "".getBytes(), TID.getBytes());
            put.addColumn("Price".getBytes(), "".getBytes(), Price.getBytes());
            put.addColumn("Yield".getBytes(), "".getBytes(), Yield.getBytes());
            put.addColumn("Sales".getBytes(), "".getBytes(), Sales.getBytes());
            table.put(put);
            System.out.println("Inset successfully!!!");
        }
        else
            System.out.println("Duplicate trade ID!!!");
    }

    public static void queryGoods(String tableName) {
        try {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Please check the products of the merchant first!!!");
            System.out.println("Please input the the merchant ID:");
            String MID = scanner.next();
            Table t = connection.getTable(TableName.valueOf(tableName));
            Filter filter = new SingleColumnValueFilter("MID".getBytes(), "".getBytes(), CompareFilter.CompareOp.EQUAL, MID.getBytes());
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner rs = t.getScanner(scan);
            for(Result r: rs) {
                for(Cell cell: r.rawCells()) {
                    System.out.print(Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.print("\t"+Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.print("\t"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.print("\t"+Bytes.toString(CellUtil.cloneValue(cell))+"\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteGoods(String tableName) {
        try {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Please input the product information you want to delete:");
            System.out.println("Please input the the trade ID:");
            String rowKey = scanner.next();
            Table t = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            if (t.exists(get)){
                Delete delete = new Delete(Bytes.toBytes(rowKey));
                t.delete(delete);
                System.out.println("Delete successfully!!!");
            }
            else {
                System.out.println("Input error,delete failed!!!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void modifyGoods(String tableName) throws IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please input the product information you want to modify:");
        System.out.println("Please input the Trade ID:");
        String TID = scanner.next();
        Get get = new Get(TID.getBytes());
        Table table = connection.getTable(TableName.valueOf(tableName));
        if(table.exists(get)){
            System.out.println("Please input the Trade name:");
            String T_name = scanner.next();
            System.out.println("Please input the Price:");
            String Price = scanner.next();
            System.out.println("Please input the Yield:");
            String Yield = scanner.next();
            Put put = new Put(TID.getBytes());
            put.addColumn("T_name".getBytes(), "".getBytes(), T_name.getBytes());
            put.addColumn("Price".getBytes(), "".getBytes(), Price.getBytes());
            put.addColumn("Yield".getBytes(), "".getBytes(), Yield.getBytes());
            table.put(put);
            System.out.println("Modify successfully!!!");
        }
        else
            System.out.println("Input error, there is no such product!!!");
    }

    public static void queryOrder(String tableName) {
        try {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Please input the the Order ID:");
            String OID = scanner.next();
            Get get = new Get(OID.getBytes());
            Table t = connection.getTable(TableName.valueOf(tableName));
            if (t.exists(get))
            {
                Filter filter = new SingleColumnValueFilter("OID".getBytes(), "".getBytes(), CompareFilter.CompareOp.EQUAL, OID.getBytes());
                Scan scan = new Scan();
                scan.setFilter(filter);
                ResultScanner rs = t.getScanner(scan);
                System.out.println("Please enter the content you want to query:");
                String x =scanner.next();
                switch (x) {
                    case "1":
                        for(Result r: rs) {
                            for (Cell cell : r.rawCells()) {
                                if (Bytes.toString(CellUtil.cloneFamily(cell)).equals("Location" ) || Bytes.toString(CellUtil.cloneFamily(cell)).equals("Money")){
                                    System.out.print(Bytes.toString(CellUtil.cloneRow(cell)));
                                    System.out.print("\t" + Bytes.toString(CellUtil.cloneFamily(cell)));
                                    System.out.print("\t" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                                    System.out.print("\t" + Bytes.toString(CellUtil.cloneValue(cell)) + "\n");
                                }
                            }
                        }
                        break;
                    case "2":
                        for(Result r: rs) {
                            for (Cell cell : r.rawCells()) {
                                System.out.print(Bytes.toString(CellUtil.cloneRow(cell)));
                                System.out.print("\t" + Bytes.toString(CellUtil.cloneFamily(cell)));
                                System.out.print("\t" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                                System.out.print("\t" + Bytes.toString(CellUtil.cloneValue(cell)) + "\n");
                            }
                        }
                        break;
                }
            }
            else
                System.out.println("Input error, there is no such order!!!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        init();
        boolean loop = true;
        while (loop) {
            System.out.println("Welcome to take-out order management system!!!");
            System.out.println("1.Add product information");
            System.out.println("2.Delete product information");
            System.out.println("3.Modify product information");
            System.out.println("4.Query for order information");
            System.out.println("5.Exit");
            Scanner scanner = new Scanner(System.in);
            String choice = scanner.next();
            switch (choice) {
                case "1":
                    System.out.println("------------------------------------------------");
                    System.out.println("Here is the interface to add product information");
                    addGoods("goods");
                    break;
                case "2":
                    System.out.println("------------------------------------------------");
                    System.out.println("Here is the interface to delete product information");
                    queryGoods("goods");
                    deleteGoods("goods");
                    break;
                case "3":
                    System.out.println("------------------------------------------------");
                    System.out.println("Here is the interface to modify product information");
                    queryGoods("goods");
                    modifyGoods("goods");
                    break;
                case "4":
                    System.out.println("------------------------------------------------");
                    System.out.println("Here is the interface to query order information");
                    System.out.println("1.Query order amount and delivery location");
                    System.out.println("2.Query order information");
                    queryOrder("order");
                    break;
                case "5":
                    loop = false;
                    System.out.println("------------------------------------------------");
                    System.out.println("Thank you for your use and successfully quit");
                    break;
            }
        }
        close();
    }
}


