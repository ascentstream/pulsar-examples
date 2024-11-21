package com.ascentstream.demo.util;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DateUtil {

    public static final DateTimeFormatter DATETIME_FORMAT_DEFAULT = DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss");
    public static final DateTimeFormatter DATE_FORMAT_DEFAULT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter TIME_FORMAT_DEFAULT = DateTimeFormatter.ofPattern("HH:mm:ss");
    public static final DateTimeFormatter DATETIME_FORMAT_DEFAULT2 = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");


    public static long getTimestamp() {
        return LocalDateTime.now().toInstant(ZoneOffset.ofHours(0)).toEpochMilli()/1000L;
    }

    public static long getTimestampMilli() {
        return LocalDateTime.now().toInstant(ZoneOffset.ofHours(0)).toEpochMilli();
    }

    public static String getDateTimeNow() {
        LocalDateTime now = LocalDateTime.now();
        return DATETIME_FORMAT_DEFAULT.format(now);
    }

    public static String getDateNow() {
        LocalDate now = LocalDate.now();
        return DATE_FORMAT_DEFAULT.format(now);
    }

    public static LocalDateTime getLocalDateTimeNow() {
        return LocalDateTime.now();
    }

    public static String formatDateTime(LocalDateTime dateTime) {
        return DATETIME_FORMAT_DEFAULT.format(dateTime);
    }

    public static String formatDate(LocalDateTime dateTime) {
        return DATE_FORMAT_DEFAULT.format(dateTime);
    }
    public static String formatDate(LocalDate date) {
        return DATE_FORMAT_DEFAULT.format(date);
    }

    public static String formatTime(LocalDateTime dateTime) {
        return TIME_FORMAT_DEFAULT.format(dateTime);
    }

    public static int getDay(LocalDateTime dateTime) {
        return dateTime.getDayOfMonth();
    }

    public static int getMonth(LocalDateTime dateTime) {
        return dateTime.getMonthValue();
    }

    public static int getYear(LocalDateTime dateTime) {
        return dateTime.getYear();
    }

    public static LocalDateTime formatLocalDateTime(String datetimeStr, DateTimeFormatter formatter) {
        return LocalDateTime.parse(datetimeStr, formatter);
    }

    public static long diffDays(LocalDateTime startTime, LocalDateTime endTime) {
        Duration duration =  Duration.between(startTime, endTime);
        return duration.toDays();
    }

}
