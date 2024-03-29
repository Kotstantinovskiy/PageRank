import java.util.regex.Pattern;

public class Config {
    public static final String URLS_PATH = "/user/b.konstantinovskij/page_rank/data/urls.txt";
    //public static final String URLS_PATH = "/home/boris/Рабочий стол/Техносфера/Hadoop/SM2/PageRank2/data/urls.txt";
    public static final String URLS_IDX_PATH = "/user/b.konstantinovskij/page_rank/data/urls_idx.txt";
    //public static final String URLS_IDX_PATH = "/home/boris/Рабочий стол/Техносфера/Hadoop/SM2/PageRank2/data/urls_idx.txt";
    public static final String HANG_RANK_PATH = "/user/b.konstantinovskij/page_rank/tmp/";
    //public static final String HANG_RANK_PATH = "/home/boris/Рабочий стол/Техносфера/Hadoop/SM2/PageRank2/tmp/";

    public static final String LENTA = "lenta.ru";
    public static final String HANGING_VERTEX = "HANG";

    public final static double D = 0.85;

    public final static double AllNums = 3286223;

    public static final int NUM_REDICERS = 5;
    public static final int ITERATIONS = 10;

    public static final Pattern HREF_PATTERN = Pattern.compile("(<a.*?href=\")(.*?)(\")");
}
