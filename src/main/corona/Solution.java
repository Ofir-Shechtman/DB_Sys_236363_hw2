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
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet results = null;

        public void prepare(String sql, int... params) throws SQLException {
            connection = DBConnector.getConnection();
            if (connection == null)
                throw new SQLException();
            pstmt = connection.prepareStatement(sql);
            if (params.length > 0)
                pstmt.setInt(1, params[0]);
            if (params.length > 1)
                pstmt.setInt(2, params[1]);
        }

        public void close() {
            Utils.closeQuietly(results);
            Utils.closeQuietly(pstmt);
            Utils.closeQuietly(connection);


        }

        public void get_record(String sql, int... params) throws SQLException {
            prepare(sql, params);
            results = pstmt.executeQuery();
            if (!results.isBeforeFirst())
                throw new SQLException();
            results.next();
        }

        public ArrayList<Integer> get_array(String sql,  int... params) throws SQLException {
            prepare(sql, params);
            results = pstmt.executeQuery();
            ArrayList<Integer> arr = new ArrayList<>();
            while (results.next()) {
                arr.add(results.getInt(1));
            }
            return arr;
        }

    }
    public static void createTables() {
        DB db= new DB();
        try {
            //db.prepare("CALL public.CreateDB();");
            db.prepare("CREATE TABLE labs\n" +
                    "(\n" +
                    "\tlabID integer,\n" +
                    "\tname text NOT NULL,\n" +
                    "\tcity text NOT NULL,\n" +
                    "\tactive bool NOT NULL,\n" +
                    "\tPRIMARY KEY (labID),\n" +
                    "\tCHECK (labID > 0)\n" +
                    ");\n" +
                    "\n" +
                    "CREATE TABLE employees\n" +
                    "(\n" +
                    "\temployeeID integer,\n" +
                    "\tname text NOT NULL,\n" +
                    "\tbirth_city text NOT NULL,\n" +
                    "\tPRIMARY KEY (employeeID),\n" +
                    "\tCHECK (employeeID > 0)\n" +
                    ");\n" +
                    "\n" +
                    "CREATE TABLE vaccines\n" +
                    "(\n" +
                    "\tvaccineID integer,\n" +
                    "\tname text NOT NULL,\n" +
                    "\tcost integer NOT NULL,\n" +
                    "\tunits_in_stock integer NOT NULL,\n" +
                    "\tproductivity integer NOT NULL,\n" +
                    "\tincome integer DEFAULT 0 NOT NULL,\n" +
                    "\tPRIMARY KEY (vaccineID),\n" +
                    "\tCONSTRAINT CHK_vaccines CHECK (vaccineID > 0 AND cost>=0 AND units_in_stock>=0 AND productivity>=0 AND productivity<=100)\n" +
                    ");\n" +
                    "\n" +
                    "CREATE TABLE working\n" +
                    "(\n" +
                    "\temployeeID integer,\n" +
                    "\tlabID integer,\n" +
                    "\tsalary integer NOT NULL,\n" +
                    "\tPRIMARY KEY (employeeID, labID),\n" +
                    "\tFOREIGN KEY (employeeID) REFERENCES employees(employeeID),\n" +
                    "\tFOREIGN KEY (labID) REFERENCES labs(labID),\n" +
                    "\tCHECK (salary >= 0)\n" +
                    ");\n" +
                    "\n" +
                    "CREATE TABLE producing\n" +
                    "(\n" +
                    "\tvaccineID integer,\n" +
                    "\tlabID integer,\n" +
                    "\tPRIMARY KEY (vaccineID, labID),\n" +
                    "\tFOREIGN KEY (vaccineID) REFERENCES vaccines(vaccineID),\n" +
                    "\tFOREIGN KEY (labID) REFERENCES labs(labID)\n" +
                    ");\n" +
                    "\n" +
                    "CREATE OR REPLACE VIEW public.v_producing AS\n" +
                    "SELECT l.labid,\n" +
                    "    l.name AS lab_name,\n" +
                    "    l.city,\n" +
                    "    l.active AS is_active,\n" +
                    "    v.vaccineid,\n" +
                    "    v.name AS vaccine_name,\n" +
                    "    v.cost,\n" +
                    "    v.units_in_stock AS units,\n" +
                    "    v.productivity,\n" +
                    "\tv.income\n" +
                    "FROM labs l\n" +
                    "INNER JOIN producing p ON l.labid = p.labid\n" +
                    "INNER JOIN vaccines v ON v.vaccineid = p.vaccineid;\n" +
                    "   \n" +
                    "CREATE OR REPLACE VIEW public.v_working AS\n" +
                    "SELECT l.labID,\n" +
                    "\t   l.name lab_name,\n" +
                    "\t   l.city,\n" +
                    "\t   l.active is_active,\n" +
                    "\t   e.employeeID,\n" +
                    "\t   e.name employee_name, \n" +
                    "\t   e.birth_city,\n" +
                    "\t   w.salary\n" +
                    "FROM public.labs l\n" +
                    "INNER JOIN public.working w ON l.labID=w.labID\n" +
                    "INNER JOIN public.employees e ON e.employeeID=w.employeeID;");
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
            //db.prepare("CALL public.TruncateDB();");
            db.prepare("TRUNCATE TABLE working;\n" +
                    "TRUNCATE TABLE producing;\n" +
                    "TRUNCATE TABLE labs CASCADE;\n" +
                    "TRUNCATE TABLE employees CASCADE;\n" +
                    "TRUNCATE TABLE vaccines CASCADE;");
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
            //db.prepare("CALL public.DropDB();");
            db.prepare("DROP VIEW public.v_producing;\n" +
                    "DROP VIEW public.v_working;\n" +
                    "DROP TABLE producing;\n" +
                    "DROP TABLE working;\n" +
                    "DROP TABLE labs;\n" +
                    "DROP TABLE employees;\n" +
                    "DROP TABLE vaccines;");
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
        Lab lab= Lab.badLab();
        try {
            db.get_record("SELECT * FROM public.labs WHERE labID=?", labID);
            lab.setId(db.results.getInt(1));
            lab.setName(db.results.getString(2));
            lab.setCity(db.results.getString(3));
            lab.setIsActive(db.results.getBoolean(4));
        } catch (SQLException ignored) {
        } finally {
            db.close();
        }
        return lab;
    }

    public static ReturnValue deleteLab(Lab lab) {
        DB db= new DB();
        ReturnValue ret=OK;
        try {
            db.prepare("DELETE FROM public.labs WHERE labID=?");
            db.pstmt.setInt(1, lab.getId());
            if(db.pstmt.executeUpdate()==0)
                ret=NOT_EXISTS;
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
        Employee employee = Employee.badEmployee();
        try {
            db.get_record("SELECT * FROM public.employees WHERE employeeID=?", employeeID);
            employee.setId(db.results.getInt(1));
            employee.setName(db.results.getString(2));
            employee.setCity(db.results.getString(3));
        } catch (SQLException ignored) {
        } finally {
           db.close();
        }
        return employee;
    }

    public static ReturnValue deleteEmployee(Employee employee) {
        DB db= new DB();
        ReturnValue ret =OK;
        try {
            db.prepare("DELETE FROM public.employees WHERE employeeID=?");
            db.pstmt.setInt(1, employee.getId());
            if(db.pstmt.executeUpdate()==0)
                ret=NOT_EXISTS;
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
        Vaccine vaccine=Vaccine.badVaccine();
        try {
            db.get_record("SELECT * FROM public.vaccines WHERE vaccineID=?", vaccineID);
            vaccine.setId(db.results.getInt(1));
            vaccine.setName(db.results.getString(2));
            vaccine.setCost(db.results.getInt(3));
            vaccine.setUnits(db.results.getInt(4));
            vaccine.setProductivity(db.results.getInt(5));
        } catch (SQLException ignored) {
        } finally {
            db.close();
        }
        return vaccine;
    }

    public static ReturnValue deleteVaccine(Vaccine vaccine) {
        DB db= new DB();
        ReturnValue ret = OK;
        try {
            db.prepare("DELETE FROM public.vaccines WHERE vaccineID=?");
            db.pstmt.setInt(1, vaccine.getId());
            if(db.pstmt.executeUpdate()==0)
                ret=NOT_EXISTS;
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
        ReturnValue ret = OK;
        try {
            db.prepare("DELETE FROM public.working WHERE labID=? AND employeeID=?");
            db.pstmt.setInt(1,labID);
            db.pstmt.setInt(2,employeeID);
            if(db.pstmt.executeUpdate()==0)
                ret=NOT_EXISTS;
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
        ReturnValue ret = OK;
        try {
            db.prepare("DELETE FROM public.producing WHERE labID=? AND vaccineID=?");
            db.pstmt.setInt(1,labID);
            db.pstmt.setInt(2,vaccineID);
            if(db.pstmt.executeUpdate()==0)
                ret=NOT_EXISTS;
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
        ReturnValue ret = OK;
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
            if(db.pstmt.executeUpdate()==0)
                ret=NOT_EXISTS;
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
        ReturnValue ret = OK;
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
            if(db.pstmt.executeUpdate()==0)
                ret=NOT_EXISTS;
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
        boolean is_popular=false;
        try {
            db.get_record("SELECT LabID\n" +
                    "FROM v_producing\n" +
                    "WHERE labID=? AND productivity<=20\n" +
                    "UNION ALL\n" +
                    "SELECT 0\n" +
                    "WHERE ? NOT IN (SELECT LabID FROM labs)", labID, labID);
        } catch (SQLException throwables) {
            is_popular= true;
        }
        db.close();
        return is_popular;
    }

    public static Integer getIncomeFromVaccine(Integer vaccineID) {
        DB db= new DB();
        int income=0;
        try {
            db.get_record("SELECT income FROM public.vaccines WHERE vaccineID=?", vaccineID);
            income=db.results.getInt(1);
        } catch (SQLException ignored) {
        } finally {
            db.close();
        }
        return income;
    }

    public static Integer getTotalNumberOfWorkingVaccines() {
        DB db= new DB();
        int tot_work=0;
        try {
            db.get_record("SELECT SUM(units_in_stock) FROM public.vaccines WHERE productivity>20");
            tot_work=db.results.getInt(1);

        } catch (SQLException ignored) {
        } finally {
            db.close();
        }
        return tot_work;
    }

    public static Integer getTotalWages(Integer labID) {
        DB db= new DB();
        int tot_work=0;
        try {
            db.get_record("SELECT SUM(salary)\n" +
                    "FROM v_working\n" +
                    "WHERE labID=? AND is_active=true\n" +
                    "HAVING COUNT(*)>1\n" +
                    "UNION ALL SELECT 0", labID);
            tot_work=db.results.getInt(1);

        } catch (SQLException ignored) {
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
            best=db.results.getInt(1);

        } catch (SQLException ignored) {
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
                    "ORDER BY COUNT(*) DESC, city ASC\n" +
                    "LIMIT 1\n");
            popular=db.results.getString(1);

        } catch (SQLException ignored) {
        } finally {
            db.close();
        }
        return popular;
    }

    public static ArrayList<Integer> getPopularLabs() {
        DB db= new DB();
        ArrayList<Integer> popular= null;
        try {
            popular=db.get_array("SELECT DISTINCT labID\n" +
                    "FROM producing\n" +
                    "EXCEPT\n" +
                    "SELECT labID\n" +
                    "FROM producing\n" +
                    "WHERE vaccineID IN (SELECT vaccineID FROM vaccines WHERE productivity<=20)\n" +
                    "ORDER BY labID ASC\n" +
                    "LIMIT 3");

        } catch (SQLException ignored) {
        } finally {
            db.close();
        }
        return popular;
    }

    public static ArrayList<Integer> getMostRatedVaccines() {
        DB db= new DB();
        ArrayList<Integer> rated= null;
        try {
            rated=db.get_array("SELECT vaccineID\n" +
                    "FROM vaccines\n" +
                    "ORDER BY productivity+units_in_stock-cost DESC\n" +
                    "LIMIT 10");

        } catch (SQLException ignored) {
        } finally {
            db.close();
        }
        return rated;
    }

    public static ArrayList<Integer> getCloseEmployees(Integer employeeID) {
        DB db= new DB();
        ArrayList<Integer> rated= null;
        try {
            rated =db.get_array("WITH working_empty_way AS(\n" +
                    "SELECT e.employeeID, w.labID\n" +
                    "FROM employees e\n" +
                    "LEFT JOIN working w\n" +
                    "ON e.employeeID=w.employeeID\n" +
                    "OR e.employeeID NOT IN (SELECT employeeID FROM working)\n" +
                    ")\n" +
                    "SELECT w2.employeeID\n" +
                    "FROM working_empty_way w1\n" +
                    "INNER JOIN working w2 ON  w1.labID=w2.labID\n" +
                    "CROSS JOIN (SELECT ? employeeID) PARAM\n" +
                    "WHERE w1.employeeID=PARAM.employeeID AND w2.employeeID!=PARAM.employeeID\n" +
                    "GROUP BY w2.employeeID, PARAM.employeeID\n" +
                    "HAVING CAST(COUNT(*) AS FLOAT)/(SELECT COUNT(DISTINCT labID) FROM working_empty_way WHERE employeeID=PARAM.employeeID)>=0.5\n" +
                    "ORDER BY w2.employeeID ASC\n" +
                    "LIMIT 10", employeeID);
        } catch (SQLException ignored) {
        } finally {
            db.close();
        }
        return rated;
    }
}

