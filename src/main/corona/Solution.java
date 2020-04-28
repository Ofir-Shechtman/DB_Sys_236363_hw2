package corona;


import corona.business.Employee;
import corona.business.Lab;
import corona.business.ReturnValue;
import corona.business.Vaccine;
import corona.data.DBConnector;
import corona.data.PostgreSQLErrorCodes;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import static corona.business.ReturnValue.*;




public class Solution {
    static class Utils {
        public static void closeQuietly(Connection connection) {
            try {
                connection.close();
            } catch (SQLException e) { // NOPMD
                // quiet
            }
        }

        public static void closeQuietly(Statement stmt) {
            try {
                stmt.close();
            } catch (SQLException e) { // NOPMD
                // quiet
            }
        }

        public static void closeQuietly(ResultSet rs) {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException e) { // NOPMD
                // quiet
            }
        }
        public static ReturnValue get_retval(SQLException e) {
            if (Integer.parseInt(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue())
                return BAD_PARAMS;
            else if (Integer.parseInt(e.getSQLState()) == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue())
                return BAD_PARAMS;
            else if (Integer.parseInt(e.getSQLState()) == PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue())
                return BAD_PARAMS;
            else if (Integer.parseInt(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
                return ALREADY_EXISTS;
            else
                return ERROR;
        }
    }

    static class DB {
        Connection connection=null;
        PreparedStatement pstmt=null;
        ResultSet results=null;

        public void prepare(String sql) throws SQLException {
            connection = DBConnector.getConnection();
            if(connection==null)
                throw new SQLException();
            pstmt = connection.prepareStatement(sql);
        }

        public void close(){
            Utils.closeQuietly(results);
            Utils.closeQuietly(pstmt);
            Utils.closeQuietly(connection);


        }

        public void get_record(String sql, int id) throws SQLException {
            prepare(sql);
            pstmt.setInt(1, id);
            results = pstmt.executeQuery();
        }

        public ReturnValue exists(String sql, int id) {
            ReturnValue r = OK;
            try {
                get_record(sql, id);
                if (results.getFetchSize() == 0)
                    r = NOT_EXISTS;
            } catch (SQLException e) {
                e.printStackTrace();
                r = ERROR;
            }
            return r;
        }
    }

    public static void createTables() {
        DB db= new DB();
        try {
            db.prepare("CALL public.CreateDB();");//TODO: copy
            db.pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            db.close();
        }
    }

    public static void clearTables() {
        DB db= new DB();
        try {
            db.prepare("CALL public.TruncateDB();");//TODO: copy
            db.pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            db.close();
        }
    }
    public static void dropTables() {
        DB db= new DB();
        try {
            db.prepare("CALL public.DropDB();");//TODO: copy
            db.pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            db.close();
        }
    }

    public static ReturnValue addLab(Lab lab) {
        ReturnValue ret=OK;
        DB db= new DB();
        try {
            db.prepare("INSERT INTO public.labs(\n" +
                    "\tlabid, name, city, active)\n" +
                    "\tVALUES (?, ?, ?, ?);");
            db.pstmt.setInt(1, lab.getId());
            db.pstmt.setString(2, lab.getName());
            db.pstmt.setString(3, lab.getCity());
            db.pstmt.setBoolean(4, lab.getIsActive());
            db.pstmt.execute();
        } catch (SQLException e) {
            ret= Utils.get_retval(e);
        }
        finally {
            db.close();
        }
        return ret;
    }

    public static Lab getLabProfile(Integer labID) {
        DB db= new DB();
        Lab lab=new Lab();
        try {
            db.get_record("SELECT * FROM public.labs WHERE labID=?", labID);
            lab.setId(db.results.getInt(1));
            lab.setName(db.results.getString(2));
            lab.setCity(db.results.getString(3));
            lab.setIsActive(db.results.getBoolean(4));
        } catch (SQLException e) {
            lab= Lab.badLab();
        } finally {
            db.close();
        }
        return lab;
    }

    public static ReturnValue deleteLab(Lab lab) {
        DB db= new DB();
        ReturnValue ret = db.exists("SELECT labID FROM public.labs WHERE labID=?", lab.getId());
        if(ret!=OK)
            return ret;
        try {
            db.prepare("DELETE FROM public.labs WHERE labID=?");
            db.pstmt.setInt(1, lab.getId());
            db.pstmt.execute();
        } catch (SQLException e) {
            ret= Utils.get_retval(e);
        }
        finally {
            db.close();
        }
        return ret;
    }

    public static ReturnValue addEmployee(Employee employee) {
        ReturnValue ret=OK;
        DB db= new DB();
        try {
            db.prepare("INSERT INTO public.employees(\n" +
                    "\temployeeid, name, birth_city)\n" +
                    "\tVALUES (?, ?, ?);");
            db.pstmt.setInt(1, employee.getId());
            db.pstmt.setString(2, employee.getName());
            db.pstmt.setString(3, employee.getCity());
            db.pstmt.execute();
        } catch (SQLException e) {
            ret= Utils.get_retval(e);
        }
        finally {
            db.close();
        }
        return ret;
    }

    public static Employee getEmployeeProfile(Integer employeeID) {
        DB db= new DB();
        Employee employee=new Employee();
        try {
            db.get_record("SELECT * FROM public.employees WHERE employeeID=?", employeeID);
            employee.setId(db.results.getInt(1));
            employee.setName(db.results.getString(2));
            employee.setCity(db.results.getString(3));
        } catch (SQLException e) {
            employee= Employee.badEmployee();
        } finally {
           db.close();
        }
        return employee;
    }

    public static ReturnValue deleteEmployee(Employee employee) {
        DB db= new DB();
        ReturnValue ret = db.exists("SELECT employeeID FROM public.employees WHERE employeeID=?", employee.getId());
        if(ret!=OK)
            return ret;
        try {
            db.prepare("DELETE FROM public.employees WHERE employeeID=?");
            db.pstmt.setInt(1, employee.getId());
            db.pstmt.execute();
        } catch (SQLException e) {
            ret= Utils.get_retval(e);
        }
        finally {
            db.close();
        }
        return ret;
    }

    public static ReturnValue addVaccine(Vaccine vaccine) {
        ReturnValue ret=OK;
        DB db= new DB();
        try {
            db.prepare("INSERT INTO public.vaccines(\n" +
                    "\tvaccineid, name, cost, units_in_stock, productivity)\n" +
                    "\tVALUES (?, ?, ?, ?, ?);");
            db.pstmt.setInt(1, vaccine.getId());
            db.pstmt.setString(2, vaccine.getName());
            db.pstmt.setInt(3, vaccine.getCost());
            db.pstmt.setInt(4, vaccine.getUnits());
            db.pstmt.setInt(5, vaccine.getProductivity());
            db.pstmt.execute();
        } catch (SQLException e) {
            ret= Utils.get_retval(e);
        }
        finally {
            db.close();
        }
        return ret;
    }

    public static Vaccine getVaccineProfile(Integer vaccineID) {
        DB db= new DB();
        Vaccine vaccine=new Vaccine();
        try {
            db.get_record("SELECT * FROM public.vaccines WHERE vaccineID=?", vaccineID);
            vaccine.setId(db.results.getInt(1));
            vaccine.setName(db.results.getString(2));
            vaccine.setCost(db.results.getInt(3));
            vaccine.setUnits(db.results.getInt(41));
            vaccine.setProductivity(db.results.getInt(5));
        } catch (SQLException e) {
            vaccine= Vaccine.badVaccine();
        } finally {
            db.close();
        }
        return vaccine;
    }

    public static ReturnValue deleteVaccine(Vaccine vaccine) {
        DB db= new DB();
        ReturnValue ret = db.exists("SELECT vaccineID FROM public.vaccines WHERE vaccineID=?", vaccine.getId());
        if(ret!=OK)
            return ret;
        try {
            db.prepare("DELETE FROM public.vaccines WHERE vaccineID=?");
            db.pstmt.setInt(1, vaccine.getId());
            db.pstmt.execute();
        } catch (SQLException e) {
            ret= Utils.get_retval(e);
        }
        finally {
            db.close();
        }
        return ret;
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

