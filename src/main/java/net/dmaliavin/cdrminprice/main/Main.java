package net.dmaliavin.cdrminprice.main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Main
{
    private static final Short CBR_TYPE = 1;
    private static final Short MSC_TYPE = 27;
    
    private static final int BATCH_SIZE = 1000;
    
    public static void main(String[] args)
    {
        try
        {
        Class.forName("org.postgresql.Driver");
        
        Connection connection = DriverManager.getConnection(
           "jdbc:postgresql://localhost:5432/db_cruise2","anaconda", "anaconda");

        insertPrimaryPrices(connection);

        Map<Short, Map<Long, Double>> rates = getRates(connection);
        
        String sqlSelect = "SELECT MP.CRUISE_DATE_RANGE_ID, MP.CURRENCY_ID, MP.PRICE_VALUE, C.COMPANY_ID "
                + " FROM CRUISE_DATE_RANGE_MIN_PRICE MP "
                + " JOIN CRUISE_DATE_RANGE USING (CRUISE_DATE_RANGE_ID)"
                + " JOIN CRUISE USING (CRUISE_ID) "
                + " JOIN VESSEL USING (VESSEL_ID) "
                + " JOIN COMPANY C USING (COMPANY_ID) ";
        Statement st = connection.createStatement();
        
        ResultSet rs = st.executeQuery(sqlSelect);
        
        String sqlInsert = "INSERT INTO "
                + "        CRUISE_DATE_RANGE_MIN_PRICE (CRUISE_DATE_RANGE_ID, PRICE_VALUE, CURRENCY_ID) "
                + "        VALUES (?, ?, ?)";
        PreparedStatement ps = connection.prepareStatement(sqlInsert);
        
        connection.setAutoCommit(false);
        int index = 0;
        while (rs.next())
        {
            Long cdrId = rs.getLong(1);
            Long currencyId = rs.getLong(2);
            Double price = rs.getDouble(3);
            Long companyId = rs.getLong(4);
            
            Map<Long, Double> currRates = null;
            if (MSC_TYPE.equals(companyId))
            {
                currRates = rates.get(MSC_TYPE);
            }
            else
            {
                currRates = rates.get(CBR_TYPE);
            }
            for (Entry<Long, Double> entry : currRates.entrySet())
            {
                if (entry.getKey().equals(currencyId))
                {
                    continue;
                }
                Double currPrice = price * currRates.get(currencyId) / entry.getValue();
                ps.setLong(1, cdrId);
                ps.setDouble(2, currPrice);
                ps.setLong(3, entry.getKey());
                ps.addBatch();
            }
            
            if (index++ % BATCH_SIZE == 0)
            {
                ps.executeBatch();
            }
            System.out.println(index);
        }
        
        ps.executeBatch();
        connection.commit();
        
        
        
        rs.close();
        st.close();
        connection.close();
        
        }
        catch(Exception e)
        {
            System.out.println(e.toString());
        }

    }
    

    
    private static void insertPrimaryPrices(Connection connection) throws SQLException
    {
        String sqlInsert = "" + 
                " WITH MIN_CABIN_PRICE AS" + 
                "(" + 
                "        SELECT CP.CRUISE_DATE_RANGE_ID, CP.CURRENCY_ID, CCP.PRICE_VALUE, " + 
                "        MIN(CCP.PRICE_VALUE) OVER (PARTITION BY CP.CRUISE_DATE_RANGE_ID) AS MIN_PRICE " + 
                "        FROM CRUISE_PRICE CP " + 
                "        JOIN CRUISE_CABIN_PRICE CCP ON CCP.CRUISE_PRICE_ID = CP.CRUISE_PRICE_ID " + 
                ")" + 
                "  INSERT INTO CRUISE_DATE_RANGE_MIN_PRICE (CRUISE_DATE_RANGE_ID, PRICE_VALUE, CURRENCY_ID)" + 
                "  SELECT DISTINCT" + 
                "       CDR.CRUISE_DATE_RANGE_ID, " + 
                "       COALESCE(MIN_CABIN_PRICE.PRICE_VALUE, C.PRICE_VALUE), " + 
                "       COALESCE(MIN_CABIN_PRICE.CURRENCY_ID, C.CURRENCY_ID)     " + 
                "  FROM CRUISE_DATE_RANGE CDR  " + 
                "  JOIN CRUISE C ON C.CRUISE_ID = CDR.CRUISE_ID " + 
                "  LEFT JOIN MIN_CABIN_PRICE ON MIN_CABIN_PRICE.CRUISE_DATE_RANGE_ID = CDR.CRUISE_DATE_RANGE_ID " + 
                "  WHERE (MIN_CABIN_PRICE.PRICE_VALUE=MIN_PRICE OR MIN_CABIN_PRICE.CRUISE_DATE_RANGE_ID IS NULL) " + 
                "    AND COALESCE(MIN_CABIN_PRICE.PRICE_VALUE, C.PRICE_VALUE) IS NOT NULL " + 
                "      AND COALESCE(MIN_CABIN_PRICE.CURRENCY_ID, C.CURRENCY_ID) IS NOT NULL " + 
                "  ORDER BY CRUISE_DATE_RANGE_ID";
        
        Statement st = connection.createStatement();
        st.execute(sqlInsert);
        
        st.close();
        
        
    }

    /**
     * @return Map<SourceType, Map<CurrencyId, RateValue>>
     * @throws SQLException 
     */
    private static Map<Short, Map<Long, Double>> getRates(Connection conn)
            throws SQLException
    {
        Map<Short, Map<Long, Double>> rates = new HashMap<Short, Map<Long, Double>>();

        String sql = "SELECT CURRENCY_ID, RATE_VALUE, SOURCE_TYPE FROM CURRENCY_EXCHANGE_RATE";

        Statement st = conn.createStatement();

        ResultSet rs = st.executeQuery(sql);

        while (rs.next())
        {
            Long currencyId = rs.getLong(1);
            Double rateValue = rs.getDouble(2);
            Short sourceType = rs.getShort(3);

            if (!rates.containsKey(sourceType))
            {
                rates.put(sourceType, new HashMap<Long, Double>());
            }
            rates.get(sourceType).put(currencyId, rateValue);
        }

        rs.close();

        st.close();

        return rates;
    }

}
