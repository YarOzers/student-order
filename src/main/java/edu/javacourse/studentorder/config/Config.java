/*
Класс для загрузка коднфигурации БД из файла dao.properties
 */
package edu.javacourse.studentorder.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    public static final String DB_URL = "db.url";
    public static final String DB_LOGIN = "db.login";
    public static final String DB_PASSWORD = "db.password";
    public static final String DB_LIMIT = "db.limit";

    private static Properties properties = new Properties();//специальная коллекция для хранения данный из конфигурационного файла

    public synchronized static String getProperty(String name) {
        if (properties.isEmpty()) {// если properties пустые, то
            try (InputStream is = Config.class.getClassLoader().getResourceAsStream("dao.properties")) {// создаем поток данных из файла

                properties.load(is);

            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        return properties.getProperty(name);// вызываем медод класса Properties, получающий свойство по ключу( в данном случае имя свойства берется из параметра).

    }
}
