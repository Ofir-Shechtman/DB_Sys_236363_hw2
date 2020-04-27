package corona;


import corona.business.Employee;
import corona.business.Lab;
import corona.business.ReturnValue;
import corona.business.Vaccine;
import corona.data.DBConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import static corona.business.ReturnValue.OK;


public class Solution {
    public static PreparedStatement prepare(String quary) throws SQLException {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        return connection.prepareStatement(quary);
    }
    public static void execute(PreparedStatement pstmt) throws SQLException {
        Connection connection = DBConnector.getConnection();
        SQLException exep=null;
        try {
            pstmt.execute();
        } catch (SQLException e) {
            exep=e;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                if(exep == null)
                    exep=e;
            }
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                if(exep == null)
                    exep=e;
            }
            if(exep!=null)
                throw exep;
        }
    }
    public static void createTables() {
        try {
            execute(prepare("CALL public.CreateDB();"));//TODO: copy
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void clearTables() {
        try {
            execute(prepare("CALL public.TruncateDB();"));//TODO: copy
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void dropTables() {
        try {
            execute(prepare("CALL public.DropDB();"));//TODO: copy
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static ReturnValue addLab(Lab lab) {
        try {
            PreparedStatement pstmt = prepare("INSERT INTO public.labs(\n" +
                    "\tlabid, name, city, active)\n" +
                    "\tVALUES (?, ?, ?, ?);");
            pstmt.setInt(1, lab.getId());
            pstmt.setString(2, lab.getName());
            pstmt.setString(3, lab.getCity());
            pstmt.setBoolean(4, lab.getIsActive());
            execute(pstmt);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return OK;
    }

    public static Lab getLabProfile(Integer labID) {
        return new Lab();
    }

    public static ReturnValue deleteLab(Lab lab) {
        try {
            PreparedStatement pstmt1 = prepare("SELECT labID FROM public.labs WHERE labID=?");
            pstmt1.setInt(1, lab.getId());
            ResultSet results = pstmt1.executeQuery();
            if(results.getFetchSize()==0)
                return ReturnValue.NOT_EXISTS;
            PreparedStatement pstmt2 = prepare("DELETE FROM public.labs WHERE labID=?");
            pstmt2.setInt(1, lab.getId());
            execute(pstmt2);
        } catch (SQLException e) {
            e.printStackTrace();
            return  ReturnValue.ERROR;

        }
        return OK;
    }

    public static ReturnValue addEmployee(Employee employee) {
        return OK;
    }

    public static Employee getEmployeeProfile(Integer employeeID) {
        return new Employee();
    }

    public static ReturnValue deleteEmployee(Employee employee) {
        return OK;
    }

    public static ReturnValue addVaccine(Vaccine vaccine) {
        return OK;
    }

    public static Vaccine getVaccineProfile(Integer vaccineID) {
        return new Vaccine();
    }

    public static ReturnValue deleteVaccine(Vaccine vaccine) {
        return OK;
    }

    public static ReturnValue employeeJoinLab(Integer employeeID, Integer labID, Integer salary) {
        return OK;
    }

    public static ReturnValue employeeLeftLab(Integer labID, Integer employeeID) {
        return OK;
    }

    public static ReturnValue labProduceVaccine(Integer vaccineID, Integer labID) {
        return OK;
    }

    public static ReturnValue labStoppedProducingVaccine(Integer labID, Integer vaccineID) {
        return OK;
    }

    public static ReturnValue vaccineSold(Integer vaccineID, Integer amount) {
        return OK;
    }

    public static ReturnValue vaccineProduced(Integer vaccineID, Integer amount) {
        return OK;
    }

    public static Boolean isLabPopular(Integer labID) {
        return true;
    }

    public static Integer getIncomeFromVaccine(Integer vaccineID) {
        return 0;
    }

    public static Integer getTotalNumberOfWorkingVaccines() {
        return 0;
    }

    public static Integer getTotalWages(Integer labID) {
        return 0;
    }

    public static Integer getBestLab() {
        return 0;
    }

    public static String getMostPopularCity() {
        return "";
    }

    public static ArrayList<Integer> getPopularLabs() {
        return new ArrayList<>();
    }

    public static ArrayList<Integer> getMostRatedVaccines() {
        return new ArrayList<>();
    }

    public static ArrayList<Integer> getCloseEmployees(Integer employeeID) {
        return new ArrayList<>();
    }
}

