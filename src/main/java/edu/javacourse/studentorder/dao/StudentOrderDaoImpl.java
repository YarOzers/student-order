package edu.javacourse.studentorder.dao;

import edu.javacourse.studentorder.config.Config;
import edu.javacourse.studentorder.domain.*;
import edu.javacourse.studentorder.exception.DaoException;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StudentOrderDaoImpl implements StudentOrderDao {
    public static final String INSERT_ORDER =
            "INSERT INTO jc_student_order(student_order_status, student_order_date, h_sur_name, h_given_name, " +
                    "h_patronymic, h_date_of_birth, h_passport_seria, h_passport_number, h_passport_date, " +
                    "h_passport_office_id, h_post_index, h_street_code, h_building, h_extension, h_apartment, " +
                    "h_university_id,h_student_number, w_sur_name, w_given_name, w_patronymic, w_date_of_birth, w_passport_seria," +
                    "w_passport_number, w_passport_date, w_passport_office_id, w_post_index, w_street_code, w_building, " +
                    "w_extension, w_apartment,w_university_id,w_student_number, certificate_id, register_office_id, marriage_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

    public static final String INSERT_CHILD = "insert into public.jc_student_child (student_order_id, c_sur_name, c_given_name, c_patronymic,\n" +
            "                                     c_date_of_birth, c_certificate_number, c_certificate_date, c_register_office_id,\n" +
            "                                     c_post_index, c_street_code, c_building, c_extension, c_apartment)\n" +
            "values (?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String SELECT_ORDERS = "SELECT so.*, ro.r_office_area_id, ro.r_office_name,\n" +
            "po_h.p_office_id as h_p_office_area_id, po_h.p_office_name as h_p_office_name,\n" +
            "po_w.p_office_id as w_p_office_area_id, po_w.p_office_name as w_p_office_name\n" +
            "FROM jc_student_order so\n" +
            "JOIN jc_register_office ro ON so.register_office_id = ro.r_office_id\n" +
            "JOIN jc_passport_office po_h ON po_h.p_office_id = so.h_passport_office_id\n" +
            "JOIN jc_passport_office po_w ON po_w.p_office_id = so.h_passport_office_id\n" +
            "WHERE student_order_status = ?\n" +
            "ORDER BY student_order_date LIMIT ?";

    public static final String SELECT_CHILD = "SELECT soc.*, ro.r_office_id,ro.r_office_name\n" +
            "FROM jc_student_child soc\n" +
            "JOIN jc_register_office ro ON ro.r_office_id = soc.c_register_office_id\n" +
            "WHERE soc.student_order_id IN ";

    private static final String SELECT_ORDERS_FULL = "SELECT so.*, ro.r_office_area_id, ro.r_office_name,\n" +
            "        po_h.p_office_id as h_p_office_area_id, po_h.p_office_name as h_p_office_name,\n" +
            "            po_w.p_office_id as w_p_office_area_id, po_w.p_office_name as w_p_office_name,\n" +
            "            soc.*, ro_c.r_office_id, ro_c.r_office_name\n" +
            "            FROM jc_student_order so\n" +
            "            JOIN jc_register_office ro ON so.register_office_id = ro.r_office_id\n" +
            "            JOIN jc_passport_office po_h ON po_h.p_office_id = so.h_passport_office_id\n" +
            "            JOIN jc_passport_office po_w ON po_w.p_office_id = so.h_passport_office_id\n" +
            "            JOIN jc_student_child soc ON soc.student_order_id = so.student_order_id\n" +
            "            JOIN jc_register_office ro_c ON ro_c.r_office_id = soc.c_register_office_id\n" +
            "            WHERE student_order_status = ?\n" +
            "            ORDER BY so.student_order_id LIMIT ?;";

    //TODO refactoring - make one method

    private Connection getConnection() throws SQLException {
       return ConnectionBuilder.getConnection();
    }


    @Override
    public Long saveStudentOrder(StudentOrder so) throws DaoException {
        Long result = -1L;

        try (Connection con = getConnection();
             PreparedStatement stmt = con.prepareStatement(INSERT_ORDER, new String[]{"student_order_id"})) {

            con.setAutoCommit(false);//отключаем автокомит для реализации транзакции

            try {
                int namberOfValues = 1;
                // Header
                stmt.setInt(namberOfValues, StudentOrderStatus.START.ordinal());
                stmt.setTimestamp(++namberOfValues, Timestamp.valueOf(LocalDateTime.now()));
                //Husband
                namberOfValues = setParamsForAdult(stmt, namberOfValues, so.getHusband());
                //Wife
                namberOfValues = setParamsForAdult(stmt, namberOfValues, so.getWife());
                //Marriage
                stmt.setString(++namberOfValues, so.getMarriageCertificateId());
                stmt.setLong(++namberOfValues, so.getMarriageOffice().getOfficeId());
                stmt.setDate(++namberOfValues, Date.valueOf(so.getMarriageDate()));
                stmt.executeUpdate();

                ResultSet gkRs = stmt.getGeneratedKeys();
                if (gkRs.next()) {
                    result = gkRs.getLong(1);
                }
                gkRs.close();

                saveChildren(con, so, result);
                // исполнение команды для модификации данных( insert, delete и тд.)


                con.commit();// комит транзакции в случае успуха
            } catch (SQLException exception) {
                con.rollback();// отмена транзакции
                throw new DaoException(exception);
            }


        } catch (SQLException exception) {
            throw new DaoException(exception);
        }
        return result;
    }

    @Override
    public List<StudentOrder> getStudentOrders() throws DaoException {
        return getStudentOrderTwoSelect();
//        return getOrderOneSelect();

    }

    private List<StudentOrder> getStudentOrderOneSelect() throws DaoException {
        List<StudentOrder> result = new LinkedList<>();  // создаем пустой лист
        try (Connection con = getConnection(); // подключение к базе , смотри что в методе getConnection
             PreparedStatement stmt = con.prepareStatement(SELECT_ORDERS_FULL)) {// создаем стейтмент

            Map<Long, StudentOrder> maps = new HashMap<>();
            stmt.setInt(1, StudentOrderStatus.START.ordinal());
            int limit = Integer.parseInt(Config.getProperty(Config.DB_LIMIT));
            stmt.setInt(2, limit);

            ResultSet rs = stmt.executeQuery();// выполняем запрос, получив множество записей
            int counter = 0;
            while (rs.next()) {
                Long soId = rs.getLong("student_order_id");
                if (!maps.containsKey(soId)) {
                    StudentOrder so = getFullStudentOrder(rs);

                    result.add(so);// добавляем заявки в лист заявок
                    maps.put(soId, so);
                }
                StudentOrder so = maps.get(soId);
                Child ch = fillChild(rs);
                so.addChild(ch);
                counter++;
            }
            if (counter >= limit) {
                result.remove(result.size() - 1);
            }
            findChildren(con, result);
            rs.close();

        } catch (SQLException exception) {
            throw new DaoException(exception);
        }
        return result;
    }


    private List<StudentOrder> getStudentOrderTwoSelect() throws DaoException {
        List<StudentOrder> result = new LinkedList<>();  // создаем пустой лист
        try (Connection con = getConnection(); // подключение к базе , смотри что в методе getConnection
             PreparedStatement stmt = con.prepareStatement(SELECT_ORDERS)) {// создаем стейтмент

            stmt.setInt(1, StudentOrderStatus.START.ordinal());
            stmt.setInt(2, Integer.parseInt(Config.getProperty(Config.DB_LIMIT)));
            ResultSet rs = stmt.executeQuery();// выполняем запрос, получив множество записей


            while (rs.next()) {
                StudentOrder so = getFullStudentOrder(rs);

                result.add(so);// добавляем заявки в лист заявок

            }
            findChildren(con, result);

        } catch (SQLException exception) {
            throw new DaoException(exception);
        }
        return result;
    }

    private StudentOrder getFullStudentOrder(ResultSet rs) throws SQLException {
        StudentOrder so = new StudentOrder(); // создаем студенческую заявку

        fillStudentOrder(rs, so);// заполняем ее данными
        fillMarriage(rs, so);

        so.setHusband(fillAdult(rs, "h_"));
        so.setWife(fillAdult(rs, "w_"));
        return so;
    }

    private void findChildren(Connection con, List<StudentOrder> result) throws SQLException {
        String cl = "(" + result.stream().map(so -> String.valueOf(so.getStudentOrderId()))
                .collect(Collectors.joining(",")) + ")";

        Map<Long, StudentOrder> maps = result.stream().collect(Collectors.toMap(so -> so.getStudentOrderId(), so -> so));
        try (PreparedStatement stmt = con.prepareStatement(SELECT_CHILD + cl)) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Child ch = fillChild(rs);
                StudentOrder so = maps.get(rs.getLong("student_order_id"));
                so.addChild(ch);
            }
        }
    }

    private Child fillChild(ResultSet rs) throws SQLException {
        String surName = rs.getString("c_sur_name");
        String givenName = rs.getString("c_given_name");
        String patronymic = rs.getString("c_patronymic");
        LocalDate dateOfBirth = rs.getDate("c_date_of_birth").toLocalDate();

        Child child = new Child(surName, givenName, patronymic, dateOfBirth);
        child.setCertificateNumber(rs.getString("c_certificate_number"));
        child.setIssueDate(rs.getDate("c_certificate_date").toLocalDate());
        Long roId = rs.getLong("c_register_office_id");
        String roArea = rs.getString("r_office_id");
        String roName = rs.getString("r_office_name");
        RegisterOffice ro = new RegisterOffice(roId, roArea, roName);
        child.setIssueDepartment(ro);

        Address address = new Address();
        Street street = new Street(rs.getLong("c_street_code"), "");
        address.setStreet(street);
        address.setPostCode(rs.getString("c_post_index"));
        address.setBuilding(rs.getString("c_building"));
        address.setExtension(rs.getString("c_extension"));
        address.setApartment(rs.getString("c_apartment"));

        child.setAddress(address);

        return child;
    }

    private Adult fillAdult(ResultSet rs, String pref) throws SQLException {
        Adult adult = new Adult();
        adult.setSurName(rs.getString(pref + "sur_name"));
        adult.setGivenName(rs.getString(pref + "given_name"));
        adult.setPatronymic(rs.getString(pref + "patronymic"));
        adult.setDateOfBirth(rs.getDate(pref + "date_of_birth").toLocalDate());
        adult.setPassportSeria(rs.getString(pref + "passport_seria"));
        adult.setPassportNumber(rs.getString(pref + "passport_number"));
        adult.setIssueDate(rs.getDate(pref + "passport_date").toLocalDate());


        Long poId = rs.getLong(pref + "passport_office_id");
        String poAred = rs.getString(pref + "p_office_area_id");
        String poName = rs.getString(pref + "p_office_name");

        PassportOffice passportOffice = new PassportOffice(rs.getLong(pref + "passport_office_id"), poAred, poName);
        adult.setIssueDepartment(passportOffice);
        Address address = new Address();
        Street street = new Street(rs.getLong(pref + "street_code"), "");
        address.setStreet(street);
        address.setPostCode(rs.getString(pref + "post_index"));
        address.setBuilding(rs.getString(pref + "building"));
        address.setExtension(rs.getString(pref + "extension"));
        address.setApartment(rs.getString(pref + "apartment"));
        adult.setAddress(address);

        University university = new University(rs.getLong(pref + "university_id"), "");
        adult.setUniversity(university);
        adult.setStudentId(rs.getString(pref + "student_number"));

        return adult;
    }

    private void fillMarriage(ResultSet rs, StudentOrder so) throws SQLException {
        so.setMarriageCertificateId(rs.getString("certificate_id"));
        so.setMarriageDate(rs.getDate("marriage_date").toLocalDate());


        Long roId = rs.getLong("register_office_id");
        String areaId = rs.getString("r_office_area_id");
        String name = rs.getString("r_office_name");
        RegisterOffice ro = new RegisterOffice(roId, areaId, name);
        so.setMarriageOffice(ro);
    }

    private void fillStudentOrder(ResultSet rs, StudentOrder so) throws SQLException {
        so.setStudentOrderId(rs.getLong("student_order_id"));
        so.setStudentOrderDate(rs.getTimestamp("student_order_date").toLocalDateTime());
        so.setStudentOrderStatus(StudentOrderStatus.fromValue(rs.getInt("student_order_status")));
    }

//    private void saveChildren(Connection con, StudentOrder so, Long soId) throws SQLException {
//        try (PreparedStatement stmt = con.prepareStatement(INSERT_CHILD)) {
//            for (Child child : so.getChildren()) {
//                stmt.setLong(1, soId);
//                setParamsForChild(stmt,child);
//                stmt.addBatch();
//            }
//            stmt.executeBatch();
//        }
//    }
    //пример кода, если помещается сразу много записей, более 10000 например

    private void saveChildren(Connection con, StudentOrder so, Long soId) throws SQLException {
        try (PreparedStatement stmt = con.prepareStatement(INSERT_CHILD)) {
            int counter = 0;
            int start = 1;
            for (Child child : so.getChildren()) {

                stmt.setLong(start, soId);

                setParamsForChild(stmt, start, child);
                stmt.addBatch();
                counter++;
                if (counter > 10000) {
                    stmt.executeBatch();
                    counter = 0;
                }
            }
            if (counter > 0) {
                stmt.executeBatch();
            }
        }
    }

    private void setParamsForChild(PreparedStatement stmt, int start, Child child) throws SQLException {
        int startContinue = setParamsForPerson(stmt, start, child);
        stmt.setString(++startContinue, child.getCertificateNumber());
        stmt.setDate(++startContinue, Date.valueOf(child.getIssueDate()));
        stmt.setLong(++startContinue, child.getIssueDepartment().getOfficeId());
        setParamsForAddress(stmt, startContinue, child);


    }

    private int setParamsForAdult(PreparedStatement stmt, int start, Adult adult) throws SQLException {
        int startContinue = setParamsForPerson(stmt, start, adult);
        stmt.setString(++startContinue, adult.getPassportSeria());
        stmt.setString(++startContinue, adult.getPassportNumber());
        stmt.setDate(++startContinue, Date.valueOf(adult.getIssueDate()));
        stmt.setLong(++startContinue, adult.getIssueDepartment().getOfficeId());
        startContinue = setParamsForAddress(stmt, startContinue, adult);
        stmt.setLong(++startContinue, adult.getUniversity().getUniversityId());
        stmt.setString(++startContinue, adult.getStudentId());
        return startContinue;


    }

    private int setParamsForAddress(PreparedStatement stmt, int start, Person person) throws SQLException {
        Address personAddress = person.getAddress();
        stmt.setString(++start, personAddress.getPostCode());
        stmt.setLong(++start, personAddress.getStreet().getStreetCode());
        stmt.setString(++start, personAddress.getBuilding());
        stmt.setString(++start, personAddress.getExtension());
        stmt.setString(++start, personAddress.getApartment());

        return start;
    }

    private int setParamsForPerson(PreparedStatement stmt, int start, Person person) throws SQLException {
        stmt.setString(++start, person.getSurName());
        stmt.setString(++start, person.getGivenName());
        stmt.setString(++start, person.getPatronymic());
        stmt.setDate(++start, Date.valueOf(person.getDateOfBirth()));
        return start;
    }
}
