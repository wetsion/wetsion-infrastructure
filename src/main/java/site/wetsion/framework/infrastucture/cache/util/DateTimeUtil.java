package site.wetsion.framework.infrastucture.cache.util;

import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.stream.IntStream;

/**
 * @author 霜华
 * @date 2022-02-09 18:16
 */
public class DateTimeUtil {

    public static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final String MINUTE_PATTERN = "yyyy-MM-dd HH:mm";
    public static final String HOUR_PATTERN = "yyyy-MM-dd HH";
    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final String MONTH_PATTERN = "yyyy-MM";
    public static final String YEAR_PATTERN = "yyyy";
    public static final String MINUTE_ONLY_PATTERN = "mm";
    public static final String HOUR_ONLY_PATTERN = "HH";
    public static final String DAY_MONTH_YEAR_PATTERN = "dd/MM/yyyy";
    public static final String TIME_PATTERN = "HH:mm:ss";
    public static final String YEAR_MONTH_DAY_PATTERN = "yyyy-MM-dd";
    public static final String BATCH_PATTERN = "yyyyMMdd";
    public static final String YEAR_MONTH_DAY_PATTERN2 = "yyyy/MM/dd";

    public static final String GMT_ZONE_ID = "GMT";

    /**
     * 字符串解析成时间对象
     *
     * @param date
     * @param pattern 如果为空，则为yyyy-MM-dd
     * @return
     */
    public static String format(Date date, String pattern) {
        if(date == null) {
            return "";
        }
        if (StringUtils.isBlank(pattern)) {
            pattern = DATE_PATTERN;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }


    /**
     * 字符串解析成时间对象
     *
     * @param dateTimeString
     * @param pattern        如果为空，则为yyyy-MM-dd
     * @return
     */
    public static Date parseDate(String dateTimeString, String pattern) {
        if (StringUtils.isBlank(pattern)) {
            pattern = DATE_PATTERN;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        try {
            return sdf.parse(dateTimeString);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将日期时间格式成只有日期的字符串（可以直接使用dateFormat，Pattern为Null进行格式化）
     *
     * @param dateTime
     * @return
     */
    public static String dateTimeToDateString(Date dateTime) {
        String dateTimeString = format(dateTime, DATE_TIME_PATTERN);
        return dateTimeString.substring(0, 10);
    }

    /**
     * 将日期时间格式成日期对象,时间转为当天零点
     *
     * @param dateTime
     * @return
     */
    public static Date dateTimeToDate(Date dateTime) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(dateTime);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }


    public static LocalDateTime dateAdd(LocalDateTime dateTime, int days) {
        if (dateTime == null) {
            dateTime = LocalDateTime.now();
        }
        return dateTime.plusDays(days);
    }

    /**
     * 指定日期{@code dataTime} 减去指定天数 {@code days}
     * @param dataTime
     * @param days
     * @return
     */
    public static LocalDateTime dateMinus(LocalDateTime dataTime, long days) {
        if (null == dataTime) {
            dataTime = LocalDateTime.now();
        }

        return dataTime.minusDays(days);
    }

    /**
     * LocalDateTime 转换为 Date.
     * @param localDateTime
     * @param zoneId time-zone ID, 如果没有指定，使用系统默认的 ZoneId
     */
    public static Date localDateTime2Date(LocalDateTime localDateTime, String zoneId){
        ZoneId zone = StringUtils.isBlank(zoneId) ? ZoneId.systemDefault() : ZoneId.of(zoneId);
        ZonedDateTime zdt = localDateTime.atZone(zone);
        return Date.from(zdt.toInstant());
    }

    /**
     * Date 转换为 LocalDateTime
     * @param date
     * @param zoneId time-zone ID, 如果没有指定，使用系统默认的 ZoneId
     */
    public static LocalDateTime date2LocalDateTime(Date date, String zoneId){
        Instant instant = date.toInstant();
        ZoneId zone = StringUtils.isBlank(zoneId) ? ZoneId.systemDefault() : ZoneId.of(zoneId);
        return instant.atZone(zone).toLocalDateTime();
    }

    /**
     * 当前时间
     * @param zoneId time-zone ID, 如果没有指定，使用系统默认的 ZoneId
     * @return
     */
    public static LocalDateTime now(String zoneId) {
        ZoneId zone = StringUtils.isBlank(zoneId) ? ZoneId.systemDefault() : ZoneId.of(zoneId);
        return LocalDateTime.now(zone);
    }

    public static Long now() {
        return System.currentTimeMillis() / 1000;
    }

    public static String format(LocalDateTime dateTime, String pattern, Locale locale) {
        if (dateTime == null) {
            dateTime = LocalDateTime.now();
        }
        if (pattern == null) {
            pattern = DATE_TIME_PATTERN;
        }
        return formatTemporal(dateTime, pattern, locale);
    }

    public static String formatTemporal(TemporalAccessor temporal, String pattern, Locale locale){
        if (locale == null){
            locale = Locale.getDefault();
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, locale);
        return formatter.format(temporal);
    }

    public static String format(LocalDateTime dateTime, String pattern) {
        return format(dateTime, pattern, Locale.getDefault());
    }

    public static String format(LocalDate date, String pattern){
        return format(date, pattern, null);
    }

    public static String format(LocalDate date, String pattern, Locale locale){
        if (date == null) {
            date = LocalDate.now();
        }
        if (pattern == null) {
            pattern = DATE_PATTERN;
        }
        return formatTemporal(date, pattern, locale);
    }

    public static String format(LocalTime time, String pattern){
        return format(time, pattern, null);
    }

    public static String format(LocalTime time, String pattern, Locale locale){
        if (time == null) {
            time = LocalTime.now();
        }
        if (pattern == null) {
            pattern = TIME_PATTERN;
        }
        return formatTemporal(time, pattern, locale);
    }


    /**
     * @param timeStr "2019-12-17 12:24:30"
     * @param pattern "yyyy-MM-dd HH:mm:ss"
     * @return
     */
    public static LocalDateTime parseDateTime(String timeStr, String pattern) {
        if (StringUtils.isBlank(timeStr)) {
            throw  new RuntimeException("timeStr = " + timeStr + ", pattern = " + pattern + " format error");
        }
        if (pattern == null) {
            pattern = DATE_TIME_PATTERN;
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return LocalDateTime.parse(timeStr, formatter);
    }

    public static LocalDate parseLocalDate(String dateStr, String pattern){
        DateTimeFormatter formatter = buildDateTimeFormatter(dateStr, pattern);
        return LocalDate.parse(dateStr, formatter);
    }

    public static LocalTime parseLocalTime(String dateStr, String pattern){
        DateTimeFormatter formatter = buildDateTimeFormatter(dateStr, pattern);
        return LocalTime.parse(dateStr, formatter);
    }

    private static DateTimeFormatter buildDateTimeFormatter(String dateStr, String pattern) {
        if (StringUtils.isBlank(dateStr)) {
            throw new DateTimeException("dateStr = " + dateStr + ", pattern = " + pattern + " format error");
        }
        if (pattern == null) {
            pattern = TIME_PATTERN;
        }
        return DateTimeFormatter.ofPattern(pattern);
    }


    /**
     * 将字符串时间转成秒级时间戳
     * @param timeStr 字符串时间，如："2020-07-08 09:14:40+03:00"
     * @param pattern 时间格式匹配，包含时区匹配，如："yyyy-MM-dd HH:mm:ssXXX"
     * @return
     */
    public static long parseTimestampWithZonePattern(String timeStr, String pattern){
        return parseTimestamp(timeStr, pattern, (ZoneId)null);
    }

    public static long parseTimestamp(Date date) {
        return date.toInstant().getEpochSecond();
    }


    /**
     * 将字符串时间转成秒级时间戳
     * @param timeStr 字符串时间，如："2019-01-01 00:00:00"
     * @param pattern 时间格式匹配，如："yyyy-MM-dd HH:mm:ss"
     * @return 返回值为秒级时间戳
     */
    public static long parseTimestamp(String timeStr, String pattern){
        return parseTimestamp(timeStr, pattern, ZoneOffset.systemDefault());
    }


    /**
     * 将字符串时间转成秒级时间戳
     * @param timeStr 字符串时间，如："2019-01-01 00:00:00"
     * @param pattern 时间格式匹配，如："yyyy-MM-dd HH:mm:ss"
     * @param zone   指定时区，如：ZoneOffset.systemDefault()、ZoneOffset.UTC
     * @return 返回值为秒级时间戳
     */
    public static long parseTimestamp(String timeStr, String pattern, ZoneId zone){
        if (StringUtils.isBlank(timeStr)) {
            throw  new RuntimeException("timeStr = " + timeStr + ", pattern = " + pattern + " format error");
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern).withZone(zone);
        TemporalAccessor t = formatter.parse(timeStr);
        return Instant.from(t).getEpochSecond();
    }

    /**
     *
     * @param timeStr
     * @return 返回值为秒级时间戳
     */
    public static long parseTimestamp(String timeStr){
        return parseTimestamp(timeStr, DATE_TIME_PATTERN);
    }

    public static long parseBatchTimestamp(String batch) {
        return parseTimestamp(batch + " 12:00:00", BATCH_PATTERN + " " + TIME_PATTERN);
    }

    /**
     *
     * @param ts 标准时间戳，秒级
     * @param pattern 返回结果的时间格式，如："yyyy-MM-dd HH:mm:ss"
     * @param zoneId 时区信息，如：UTC
     * @return 返回值为按照pattern格式输出的指定时区的时间
     */
    public static String fromTimestamp(long ts, String pattern, ZoneId zoneId){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        ZoneOffset zoneOffset = OffsetDateTime.now(zoneId).getOffset();
        OffsetDateTime time = Instant.ofEpochSecond(ts).atOffset(zoneOffset);
        return formatter.format(time);
    }

    /**
     *
     * @param ts 标准时间戳，秒级
     * @param pattern 返回结果的时间格式，如："yyyy-MM-dd HH:mm:ss"
     * @return 返回值为按照pattern格式输出系统默认时区的时间
     */
    public static String fromTimestamp(long ts, String pattern){
        return fromTimestamp(ts, pattern, ZoneOffset.systemDefault());
    }

    public static Integer getCurrentTimeInteger(){
        return  Long.valueOf(System.currentTimeMillis()/1000+"").intValue();
    }

    public static String getPackageBatch(Long ts) {
        ts = ts - 12 * 3600;
        return fromTimestamp(ts, "yyyyMMdd", ZoneId.of("Asia/Shanghai"));
    }

    public static String getPackageBatch() {
        return getPackageBatch(Instant.now().getEpochSecond());
    }

    public static String parseTimestamp(String timeStr, String pattern, TimeZone timeZone){
        if (StringUtils.isBlank(timeStr)) {
            throw  new RuntimeException("timeStr = " + timeStr + ", pattern = " + pattern + " format error");
        }

        DateFormat formatter = new SimpleDateFormat(pattern);
        formatter.setTimeZone(timeZone);
        try {
            Date date = formatter.parse(timeStr);
            return format(date, pattern);
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 将时间戳转成date对象
     * @param timestamp 秒级时间戳
     * @return
     */
    public static Date fromTimestamp(Long timestamp) {
        return new Date(timestamp * 1000);
    }

    public static Date getDateFromDateStr(String dateStr, Integer defaultOffsetSec) {
        Date date;
        if(StringUtils.isEmpty(dateStr)) {
            date = new Date(System.currentTimeMillis() + defaultOffsetSec * 1000);
        } else {
            date = DateTimeUtil.parseDate(dateStr, "yyyyMMdd");
        }

        return date;
    }

    public static Date getDateAfter(Integer days, Date now) {
        Calendar c = Calendar.getInstance();
        c.setTime(now);
        c.add(Calendar.DATE, days);
        return c.getTime();
    }

    public static List<String> getDateListBetween(Date start, Date end, String timezone, String format) {
        LocalDateTime startDay = DateTimeUtil.date2LocalDateTime(start, timezone);
        long interval = ChronoUnit.DAYS.between(startDay, DateTimeUtil.date2LocalDateTime(end, timezone));
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(format);
        List<String> result = new ArrayList<>();
        IntStream.iterate(0, i -> i + 1)
                .limit(interval)
                .mapToObj(i -> startDay.toLocalDate().plusDays(i))
                .forEach(date -> result.add(date.format(dtf)));
        return result;
    }


    /**
     * 获得某天最大时间 例如：2017-10-15 23:59:59
     * @param date
     * @return
     */
    public static Date getEndOfDay(Date date) {
        return getEndOfDay(date, ZoneId.systemDefault());
    }

    /**
     * 获得某天最大时间 例如：2017-10-15 23:59:59
     * @param date
     * @param zoneId
     * @return
     */
    public static Date getEndOfDay(Date date, ZoneId zoneId) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()), zoneId);;
        LocalDateTime endOfDay = localDateTime.with(LocalTime.MAX);
        return Date.from(endOfDay.atZone(zoneId).toInstant());
    }

    /**
     * 获得某天最小时间 例如：2017-10-15 00:00:00
     * @param date
     * @return
     */
    public static Date getBeginOfDay(Date date) {
        return getBeginOfDay(date, ZoneId.systemDefault());
    }

    /**
     * 获得某天最小时间 2017-10-15 00:00:00
     * @param date
     * @param zoneId
     * @return
     */
    public static Date getBeginOfDay(Date date, ZoneId zoneId) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()), zoneId);
        LocalDateTime beginOfDay = localDateTime.with(LocalTime.MIN);
        return Date.from(beginOfDay.atZone(zoneId).toInstant());
    }

    /**
     * 获取时间戳对应的当天开始时间，时区为系统默认时区
     * @param ts 时间戳（秒）
     * @return
     */
    public static Long getBeginOfDayTimestamp(Long ts) {
        return getBeginOfDayTimestamp(ts, ZoneId.systemDefault());
    }

    /**
     * 获取时间戳对应的当天开始时间，时区为指定时区
     * @param ts 时间戳（秒）
     * @param zoneId 指定时区
     * @return
     */
    public static Long getBeginOfDayTimestamp(Long ts, ZoneId zoneId) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(ts), zoneId);
        LocalDateTime beginOfDay = localDateTime.with(LocalTime.MIN);
        return beginOfDay.atZone(zoneId).toInstant().getEpochSecond();
    }

    /**
     * 获取时间戳对应的当天结束时间，时区为指定时区
     * @param ts 时间戳（秒）
     * @param zoneId 指定时区
     * @return
     */
    public static Long getEndOfDayTimestamp(Long ts, ZoneId zoneId) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(ts), zoneId);
        LocalDateTime beginOfDay = localDateTime.with(LocalTime.MAX);
        return beginOfDay.atZone(zoneId).toInstant().getEpochSecond();
    }

    /**
     * 判断是否是有效的日期
     * @param str 日期
     * @param format 指定日期格式
     */
    public static boolean isValidDate(String str, String format) {

        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
            // 设置lenient为false. 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
            dateFormat.setLenient(false);
            dateFormat.parse(str);
            return true;
        } catch (ParseException e) {
            // 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
            return false;
        }
    }


    /**
     * 将ISO标准时间字符串转换为Instant对象
     * @param cs ISO标准时间字符串，如：2020-03-05T10:52:25Z 或者 2020-03-05T13:52:25+03:00，具体兼容格式参见：@DateTimeFormatter.ISO_DATE_TIME
     * @return
     */
    public static Instant parseISODateTime(String cs){
        return DateTimeFormatter.ISO_DATE_TIME.parse(cs, Instant::from);
    }

    /**
     * 获取 RFC1123 格式的时间：比如“Fri, 26 Feb 2021 07:34:11 GMT”
     * @param timeZone
     * @return
     */
    public static String getRFC1123Time(String timeZone) {
        String zone = StringUtils.isBlank(timeZone) ? GMT_ZONE_ID : timeZone;
        SimpleDateFormat sdf3 = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
        sdf3.setTimeZone(TimeZone.getTimeZone(zone));
        return sdf3.format(new Date());
    }

    public static Date parseRFC1123Time(String timeZone, String rfcTime) throws ParseException {
        String zone = StringUtils.isBlank(timeZone) ? GMT_ZONE_ID : timeZone;
        SimpleDateFormat sdf = new SimpleDateFormat("MMM d, yyyy K:m:s a", Locale.ENGLISH);
        sdf.setTimeZone(TimeZone.getTimeZone(zone));
        return sdf.parse(rfcTime);
    }

    /**
     * 获取
     * @param datetime
     * @param format
     * @return
     */
    public static String dateToWeek(String datetime, String format) {
        String[] weekDays = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
            Date date = sdf.parse(datetime);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
            return weekDays[w];
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    public static String getWeekMonday(String dateStr) {
        Date date = parseDate(dateStr,DateTimeUtil.BATCH_PATTERN);
        SimpleDateFormat formater = new SimpleDateFormat(DateTimeUtil.BATCH_PATTERN);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int day_of_week = cal.get(Calendar.DAY_OF_WEEK) - 1;

        if (day_of_week == 0)

            day_of_week = 7;

        cal.add(Calendar.DATE, -day_of_week +1);

        return formater.format(cal.getTime());
    }

    /**
     * date 20200101
     * @return  202001W1
     */
    public static String dateToMonthWeek(String dateStr){

        // SimpleDateFormat sdf = new SimpleDateFormat(DateTimeUtil.BATCH_PATTERN);
        try {
            String month = dateStr.substring(0,6);
            Date date = parseDate(dateStr,DateTimeUtil.BATCH_PATTERN);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.DAY_OF_MONTH,-1);
            int w = cal.get(Calendar.DAY_OF_WEEK_IN_MONTH) ;
            return month+"W"+w;
        } catch (Exception e) {
            return dateStr;
            //throw new RuntimeException(e.getMessage());

        }


    }


    /**
     * date 20200101
     * @return  202001W1
     */
    public static String dateToMonthMonth(String dateStr){
        try{
            String month = dateStr.substring(0,6);
            return month;
        }catch(Exception e){

        }
        return dateStr;

    }

    public static Long getTodayZeroTimestamp() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis() / 1000;
    }

    /**
     * 获取上个月的年月
     */
    public static String getLastMonth() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
        Date date = new Date();
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, -1);
        return sdf.format(cal.getTime());
    }

}
