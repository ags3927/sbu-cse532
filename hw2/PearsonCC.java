import java.sql.*;
import java.util.*;
import java.io.*;
import java.lang.*;

public class PearsonCoefficient
{
    static
    {
        try
        {
            Class.forName("COM.ibm.db2.jdbc.app.DB2Driver");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

	private Connection connection = null;
	private Statement statement1 =null, statement2 = null;

    private double computePearsonCC(String dbname, String username, String password, String stock1Name, String stock2Name)
    {
        Formatter formatter = new Formatter();

        double s1_cp = 0.0;
        double s1_cp_sq = 0.0;
        double total_s1_cp = 0.0;
        double total_s1_cp_sq = 0.0;

        double s2_cp = 0.0;
        double s2_cp_sq = 0.0;
        double total_s2_cp = 0.0;
        double total_s2_cp_sq = 0.0;

        double s1_s2_cp_mul = 0.0;
        double total_s1_s2_cp_mul = 0.0;
        
        int n = 0;

        double n_sum_s1_s2 = 0.0;
        double prod_sum_s1_sum_s2 = 0.0;
        
        double n_sum_s1_sq = 0.0;
        double sq_sum_s1 = 0.0;

        double n_sum_s2_sq = 0.0;
        double sq_sum_s2 = 0.0;

        double numerator = 0.0;
        double denominator = 0.0;

        double pearsonCC = 0.0;

        try {
            connection = DriverManager.getConnection(dbname, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }

        ResultSet result1 = null, result2 = null;

        String query1 = "SELECT ClosingPrice From cse532.stock WHERE StockName = '" + Stock1Name + "' ORDER BY Date";
        String query2 = "SELECT ClosingPrice From cse532.stock WHERE StockName = '" + Stock2Name + "' ORDER BY Date";

        try {
            statement1 = connection.createStatement();
            statement2 = connection.createStatement();
            result1 = statement1.executeQuery(query1);
            result2 = statement2.executeQuery(query2);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        List <Double> list1 = new ArrayList<Double>();
        List <Double> list2 = new ArrayList<Double>();

        try {
            while (result1.next())
            {
                list1.add(result1.getDouble(1));
            }
            result1.close();
            statement1.close();

            while (result2.next())
            {
                list2.add(result2.getDouble(1));
            }
            result2.close();
            statement2.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        n = list1.size();

        for (int i = 0; i < n; i++)
        {
            // Get the x and y values
            s1_cp = list1.get(i);
            s2_cp = list2.get(i);

            // Compute x^2 and y^2
            s1_cp_sq = s1_cp * s1_cp;
            s2_cp_sq = s2_cp * s2_cp;

            // Compute xy
            s1_s2_cp_mul = s1_cp * s2_cp;

            // Add the x to sum
            total_s1_cp += s1_cp;

            // Add x^2 to sum
            total_s1_cp_sq += s1_cp_sq;

            // Add the y to sum
            total_s2_cp += s2_cp;

            // Add y^2 to sum
            total_s2_cp_sq += s2_cp_sq;

            // Add xy to sum
            total_s1_s2_cp_mul += s1_s2_cp_mul;
        }

        // The term for y in the denominator
        n_sum_s2_sq = n * total_s2_cp_sq;
        sq_sum_s2 = total_s2_cp * total_s2_cp;

        // The term for x in the denominator
        n_sum_s1_sq = n * total_s1_cp_sq;
        sq_sum_s1 = total_s1_cp * total_s1_cp;

        // The denominator
        denominator = Math.sqrt((n_sum_s1_sq - sq_sum_s1) * (n_sum_s2_sq - sq_sum_s2));

        // The numerator
        n_sum_s1_s2 = n * total_s1_s2_cp_mul;
        prod_sum_s1_sum_s2 = total_s1_cp * total_s2_cp;
        numerator = n_sum_s1_s2 - prod_sum_s1_sum_s2;

        // The Pearson Correlation Coefficient
        pearsonCC = numerator / denominator;

        return pearsonCC;
    }

    public static void main(String[] args)
    {
        PearsonCoefficient pearsonCC = new PearsonCoefficient();

        if (argv.length != 5)
        {
            System.out.println("Usage: java PearsonCoefficient <dbname> <username> <password> <stock1Name> <stock2Name>");
            System.exit(1);
        }

        String dbname = argv[0];
        String username = argv[1];
        String password = argv[2];
        String stock1Name = argv[3];
        String stock2Name = argv[4];

        double pearsonCCValue = pearsonCC.computePearsonCC(dbname, username, password, stock1Name, stock2Name);

        double scale = Math.pow(10, 4);


        System.out.println("Pearson Correlation Coefficient for the closing prices of " + stock1Name + " and " + stock2Name + " = " + Math.round(pearsonCCValue * scale) / scale);
    }

}