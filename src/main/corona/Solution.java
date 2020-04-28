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
                return NOT_EXISTS;
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

        public void get_record(String sql, int ... params) throws SQLException {
            prepare(sql);
            if(params.length>0)
                pstmt.setInt(1, params[0]);
            if(params.length>1)
                pstmt.setInt(2, params[1]);
            results = pstmt.executeQuery();
        }

        public ReturnValue exists(String sql, int ... params ) {
            ReturnValue r = OK;
            try {
                get_record(sql, params);
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
        ReturnValue ret=OK;
        DB db= new DB();
        try {
            db.prepare("INSERT INTO public.working(\n" +
                    "\temployeeID, labID, salary)\n" +
                    "\tVALUES (?, ?, ?);");
            db.pstmt.setInt(1, employeeID);
            db.pstmt.setInt(2, labID);
            db.pstmt.setInt(3, salary);
            db.pstmt.execute();
        } catch (SQLException e) {
            ret= Utils.get_retval(e);
        }
        finally {
            db.close();
        }
        return ret;
    }

    public static ReturnValue employeeLeftLab(Integer labID, Integer employeeID){
        DB db= new DB();
        ReturnValue ret = db.exists("SELECT * FROM public.working WHERE labID=? AND employeeID=?", labID, employeeID);
        if(ret!=OK)
            return ret;
        try {
            db.prepare("DELETE FROM public.working WHERE labID=? AND employeeID=?");
            db.pstmt.setInt(1,labID);
            db.pstmt.setInt(1,employeeID);
            db.pstmt.execute();
        } catch (SQLException e) {
            ret= Utils.get_retval(e);
        }
        finally {
            db.close();
        }
        return ret;
    }

    public static ReturnValue labProduceVaccine(Integer vaccineID, Integer labID) {
        ReturnValue ret=OK;
        DB db= new DB();
        try {
            db.prepare("INSERT INTO public.producing(\n" +
                    "\tvaccineID, labID)\n" +
                    "\tVALUES (?, ?);");
            db.pstmt.setInt(1, vaccineID);
            db.pstmt.setInt(2, labID);
            db.pstmt.execute();
        } catch (SQLException e) {
            ret= Utils.get_retval(e);
        }
        finally {
            db.close();
        }
        return ret;
    }

    public static ReturnValue labStoppedProducingVaccine(Integer labID, Integer vaccineID) {
        DB db= new DB();
        ReturnValue ret = db.exists("SELECT * FROM public.producing WHERE labID=? AND vaccineID=?", labID, vaccineID);
        if(ret!=OK)
            return ret;
        try {
            db.prepare("DELETE FROM public.producing WHERE labID=? AND vaccineID=?");
            db.pstmt.setInt(1,labID);
            db.pstmt.setInt(2,vaccineID);
            db.pstmt.execute();
        } catch (SQLException e) {
            ret= Utils.get_retval(e);
        }
        finally {
            db.close();
        }
        return ret;
    }

    public static ReturnValue vaccineSold(Integer vaccineID, Integer amount){
        DB db= new DB();
        ReturnValue ret = db.exists("SELECT * FROM public.vaccines WHERE vaccineID=?", vaccineID);
        if(ret!=OK)
            return ret;
        try {
            db.prepare("UPDATE public.vaccines SET (productivity, units_in_stock, cost, income) =\n" +
                    "    (SELECT CASE WHEN productivity<85 THEN productivity+15\tELSE 100 END,\n" +
                    "\t \t\tunits_in_stock-?,\n" +
                    "\t \t\tcost*2,\n" +
                    "\t \t\tincome+?*cost\n" +
                    "\t FROM public.vaccines\n" +
                    "     WHERE vaccineID=?)\n" +
                    "WHERE vaccineID=?;");
            db.pstmt.setInt(1,amount);
            db.pstmt.setInt(2,amount);
            db.pstmt.setInt(3,vaccineID);
            db.pstmt.setInt(4,vaccineID);
            db.pstmt.execute();
        } catch (SQLException e) {
            ret= Utils.get_retval(e);
        }
        finally {
            db.close();
        }
        return ret;
    }

    public static ReturnValue vaccineProduced(Integer vaccineID, Integer amount) {
        if(amount<0)
            return BAD_PARAMS;
        DB db= new DB();
        ReturnValue ret = db.exists("SELECT * FROM public.vaccines WHERE vaccineID=?", vaccineID);
        if(ret!=OK)
            return ret;
        try {
            db.prepare("UPDATE public.vaccines SET (productivity, units_in_stock, cost) =\n" +
                    "    (SELECT CASE WHEN productivity>15 THEN productivity-15\tELSE 0 END,\n" +
                    "\t \t\tunits_in_stock+?,\n" +
                    "\t \t\tcost/2\n" +
                    "\t FROM public.vaccines\n" +
                    "     WHERE vaccineID=?)\n" +
                    "WHERE vaccineID=?;");
            db.pstmt.setInt(1,amount);
            db.pstmt.setInt(2,vaccineID);
            db.pstmt.setInt(3,vaccineID);
            db.pstmt.execute();
        } catch (SQLException e) {
            ret= Utils.get_retval(e);
        }
        finally {
            db.close();
        }
        return ret;
    }

    public static Boolean isLabPopular(Integer labID) {
        DB db= new DB();
        ReturnValue ret = db.exists("SELECT *\n" +
                "FROM v_producing\n" +
                "WHERE labID=? AND productivity<=20", labID);
        boolean is_popular=ret==NOT_EXISTS;
        db.close();
        return is_popular;
    }

    public static Integer getIncomeFromVaccine(Integer vaccineID) {
        DB db= new DB();
        int income=0;
        try {
            db.get_record("SELECT income FROM public.vaccines WHERE vaccineID=?", vaccineID);
            if(db.results.getFetchSize()>0)
                income=db.results.getInt(1);

        } catch (SQLException e) {
            //income= 0;
        } finally {
            db.close();
        }
        return income;
    }

    public static Integer getTotalNumberOfWorkingVaccines() {
        DB db= new DB();
        int tot_work=0;
        try {
            db.get_record("SELECT COUNT(*) FROM public.vaccines WHERE productivity>20");
            if(db.results.getFetchSize()>0)
                tot_work=db.results.getInt(1);

        } catch (SQLException e) {
            //tot_work= 0;
        } finally {
            db.close();
        }
        return tot_work;
    }

    public static Integer getTotalWages(Integer labID) {
        DB db= new DB();
        int tot_work=0;
        try {
            db.get_record("SELECT SUM(salary) FROM FROM v_workingWHERE labID=?", labID);
            if(db.results.getFetchSize()>0)
                tot_work=db.results.getInt(1);

        } catch (SQLException e) {
            //tot_work= 0;
        } finally {
            db.close();
        }
        return tot_work;
    }

    public static Integer getBestLab() {
        DB db= new DB();
        int best=0;
        try {
            db.get_record("SELECT labID\n" +
                    "FROM v_working\n" +
                    "WHERE city=birth_city\n" +
                    "GROUP BY labID\n" +
                    "ORDER BY COUNT(*) DESC, labID ASC\n" +
                    "LIMIT 1");
            if(db.results.getFetchSize()>0)
                best=db.results.getInt(1);

        } catch (SQLException e) {
            //tot_work= 0;
        } finally {
            db.close();
        }
        return best;
    }

    public static String getMostPopularCity() {
        DB db= new DB();
        String popular="";
        try {
            db.get_record("SELECT city\n" +
                    "FROM v_working\n" +
                    "GROUP BY city\n" +
                    "ORDER BY COUNT(*) DESC, city DESC\n" +
                    "LIMIT 1\n");
            if(db.results.getFetchSize()>0)
                popular=db.results.getString(1);

        } catch (SQLException e) {
            //tot_work= 0;
        } finally {
            db.close();
        }
        return popular;
    }

    public static ArrayList<Integer> getPopularLabs() {
        DB db= new DB();
        ArrayList<Integer> popular= new ArrayList<Integer>();
        try {
            db.get_record("SELECT DISTINCT labID\n" +
                    "FROM producing\n" +
                    "EXCEPT\n" +
                    "SELECT labID\n" +
                    "FROM producing\n" +
                    "WHERE vaccineID IN (SELECT vaccineID FROM vaccines WHERE productivity<=20)\n" +
                    "ORDER BY labID ASC\n" +
                    "LIMIT 3");
            if(db.results.getFetchSize()>0)
                popular= (ArrayList<Integer>) db.results.getArray(1);

        } catch (SQLException e) {
            //tot_work= 0;
        } finally {
            db.close();
        }
        return popular;
    }

    public static ArrayList<Integer> getMostRatedVaccines() {
        DB db= new DB();
        ArrayList<Integer> rated= new ArrayList<Integer>();
        try {
            db.get_record("SELECT vaccineID\n" +
                    "FROM vaccines\n" +
                    "ORDER BY productivity+units_in_stock-cost DESC\n" +
                    "LIMIT 10");
            if(db.results.getFetchSize()>0)
                rated= (ArrayList<Integer>) db.results.getArray(1);

        } catch (SQLException e) {
            //tot_work= 0;
        } finally {
            db.close();
        }
        return rated;
    }

    public static ArrayList<Integer> getCloseEmployees(Integer employeeID) {
        DB db= new DB();
        ArrayList<Integer> rated= new ArrayList<Integer>();
        try {
            db.get_record("SELECT w2.employeeID\n" +
                    "FROM working w1\n" +
                    "INNER JOIN working w2 ON  w1.labID=w2.labID\n" +
                    "WHERE w1.employeeID=1 AND w2.employeeID!=1\n" +
                    "GROUP BY w2.employeeID\n" +
                    "HAVING CAST(COUNT(*) AS FLOAT)/(SELECT COUNT(*) FROM working WHERE employeeID=1)>=0.5\n" +
                    "ORDER BY w2.employeeID ASC\n" +
                    "LIMIT 10");
            if(db.results.getFetchSize()>0)
                rated= (ArrayList<Integer>) db.results.getArray(1);

        } catch (SQLException e) {
            //tot_work= 0;
        } finally {
            db.close();
        }
        return rated;
    }
}

