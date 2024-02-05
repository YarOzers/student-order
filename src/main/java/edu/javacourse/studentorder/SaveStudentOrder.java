package edu.javacourse.studentorder;

import edu.javacourse.studentorder.dao.DictionaryDaoImpl;
import edu.javacourse.studentorder.dao.StudentOrderDaoImpl;
import edu.javacourse.studentorder.dao.StudentOrderDao;
import edu.javacourse.studentorder.domain.*;
import edu.javacourse.studentorder.exception.DaoException;

import java.sql.*;
import java.time.LocalDate;
import java.util.List;

public class SaveStudentOrder {
    public static void main(String[] args) throws ClassNotFoundException, SQLException, DaoException {

//        List<Street> streets = new DictionaryDaoImpl().findStreet("");
//        for(Street street : streets) {
//            System.out.println(street.getStreetName());
//        }
//        List<PassportOffice> passportOffices = new DictionaryDaoImpl().findPassportOffice("010020000000");
//        for(PassportOffice passportOffice: passportOffices){
//            System.out.println(passportOffice.getOfficeName());
//        }
//
//        List<RegisterOffice> registerOffices = new DictionaryDaoImpl().findRegisterOffice("010010000000");
//        for(RegisterOffice registerOffice: registerOffices){
//            System.out.println(registerOffice.getOfficeName());
//        }
//
//        List<CountryArea> ca1 = new DictionaryDaoImpl().findAreas("");
//        for(CountryArea c:ca1){
//            System.out.println(c.getAreaId() + " : "+ c.getAreaName());
//        }
//
//        List<CountryArea> ca2 = new DictionaryDaoImpl().findAreas("020000000000");
//        for(CountryArea c:ca2){
//            System.out.println(c.getAreaId() + " : "+ c.getAreaName());
//        }
//
//        List<CountryArea> ca3 = new DictionaryDaoImpl().findAreas("020010000000");
//        for(CountryArea c:ca3){
//            System.out.println(c.getAreaId() + " : "+ c.getAreaName());
//        }
//
//        List<CountryArea> ca4 = new DictionaryDaoImpl().findAreas("020010010000");
//        for(CountryArea c:ca4){
//            System.out.println(c.getAreaId() + " : "+ c.getAreaName());
//        }


        StudentOrder s = buildStudentOrder(10);
        StudentOrderDao dao = new StudentOrderDaoImpl();
        Long id = dao.saveStudentOrder(s);

        List<StudentOrder> soList = dao.getStudentOrders();
        for (StudentOrder so : soList) {
            System.out.println(so.getStudentOrderId());
        }
//        StudentOrder so = new StudentOrder();
//        long ans = saveStudentOrder(so);
//        System.out.println(ans);
    }

    static long saveStudentOrder(StudentOrder studentOrder) {
        long answer = 199;
//        System.out.println("saveStudentOrder");

        return answer;
    }

    public static StudentOrder buildStudentOrder(long id) {
        StudentOrder so = new StudentOrder();
        so.setStudentOrderId(id);
        so.setMarriageCertificateId("" + (123456000 + id));
        so.setMarriageDate(LocalDate.of(2016, 7, 4));
        RegisterOffice registerOffice = new RegisterOffice(1L, "", "");
        so.setMarriageOffice(registerOffice);

        Street street = new Street(1L, "First Street");

        Address address = new Address("195000", street, "12", "", "142");

        // Муж
        Adult husband = new Adult("Bob", "Morley", "Shumacher", LocalDate.of(1997, 8, 24));
        husband.setPassportSeria("" + (1000 + id));
        husband.setPassportNumber("" + (100000 + id));
        husband.setIssueDate(LocalDate.of(2017, 9, 15));
        PassportOffice passportOffice1 = new PassportOffice(1L, "", "");
        husband.setIssueDepartment(passportOffice1);
        husband.setStudentId("" + (100000 + id));
        husband.setAddress(address);
        husband.setUniversity(new University(1L, ""));
        husband.setStudentId("HH1324");
        // Жена
        Adult wife = new Adult("Anna", "Elisabet", "Taylor", LocalDate.of(1998, 3, 12));
        wife.setPassportSeria("" + (2000 + id));
        wife.setPassportNumber("" + (200000 + id));
        wife.setIssueDate(LocalDate.of(2018, 4, 5));
        PassportOffice passportOffice2 = new PassportOffice(1L, "", "");
        wife.setIssueDepartment(passportOffice2);
        wife.setStudentId("" + (200000 + id));
        wife.setAddress(address);
        wife.setUniversity(new University(1L, ""));
        wife.setStudentId("JJ1345");
        // Ребенок
        Child child1 = new Child("Mikey", "Honeyken", "Junior", LocalDate.of(2018, 6, 29));
        child1.setCertificateNumber("" + (300000 + id));
        child1.setIssueDate(LocalDate.of(2018, 6, 11));
        RegisterOffice registerOffice2 = new RegisterOffice(2L, "", "");
        child1.setIssueDepartment(registerOffice2);
        child1.setAddress(address);
        // Ребенок
        Child child2 = new Child("Maya", "Summer", "Junior", LocalDate.of(2018, 6, 29));
        child2.setCertificateNumber("" + (400000 + id));
        child2.setIssueDate(LocalDate.of(2018, 7, 19));
        RegisterOffice registerOffice3 = new RegisterOffice(3L, "", "");
        child2.setIssueDepartment(registerOffice3);
        child2.setAddress(address);

        so.setHusband(husband);
        so.setWife(wife);
        so.addChild(child1);
        so.addChild(child2);

        return so;
    }
}
