package com.cjie.storm.trident.trend;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: hucj
 * Date: 14-6-25
 * Time: 上午9:43
 * To change this template use File | Settings | File Templates.
 */

public class EWMA implements Serializable {
    public static enum Time {
        MILLISECONDS(1),
        SECONDS(1000),
        MINUTES(SECONDS.getTime() * 60),
        HOURS(MINUTES.getTime() * 60),
        DAYS(HOURS.getTime() * 24),
        WEEKS(DAYS.getTime() * 7);
        private long millis;
        private Time(long millis) {
            this.millis = millis;
        }
        public long getTime() {
            return this.millis;
        }
    }

    // Unix load average-style alpha constants
    
    public static final double ONE_MINUTE_ALPHA = 1 - Math.exp(-5d / 60d / 1d);
    public static final double FIVE_MINUTE_ALPHA = 1 - Math.exp(-5d / 60d / 5d);
    public static final double FIFTEEN_MINUTE_ALPHA =  1 - Math.exp(-5d / 60d / 15d);
    private long window;
    private long alphaWindow;
    private long last;
    private double average;
    private double alpha = -1D;
    private boolean sliding = false;
    public EWMA() {
    }

    public EWMA sliding(double count, Time time) {
        return this.sliding((long) (time.getTime() * count));
    }
    public EWMA sliding(long window) {
        this.sliding = true;
        this.window = window;
        return this;
    }

    public EWMA withAlpha(double alpha) {
        if (!(alpha > 0.0D && alpha <= 1.0D)) {
            throw new IllegalArgumentException("Alpha must be between 0.0 and 1.0");
        }
        this.alpha = alpha;
        return this;
    }
    public EWMA withAlphaWindow(long alphaWindow) {
        this.alpha = -1;
        this.alphaWindow = alphaWindow;
        return this;
    }

    public EWMA withAlphaWindow(double count, Time
            time) {
        return this.withAlphaWindow((long) (time.getTime() * count));
    }
    public void mark() {
        mark(System.currentTimeMillis());
    }

    /**
     * 更新移动平均值，如果没有参数，则使用当前时间来计算平均值
     * @param time
     */
    public synchronized void mark(long time) {
        if (this.sliding) {
            if (time - this.last > this.window) {
                // reset the sliding window
                this.last = 0;
            }
        }
        if (this.last == 0) {
            this.average = 0;
            this.last = time;
        }
        long diff = time - this.last;
        double alpha = this.alpha != -1.0 ? this.alpha :
                Math.exp(-1.0 * ((double) diff / this.alphaWindow));
        this.average = (1.0 - alpha) * diff + alpha * this.average;
        this.last = time;
    }
    /**
     * 返回mark()方法多次调用的平均间隔时间，单位是微秒
     * @return
     */
    public double getAverage() {
        return this.average;
    }
    /**
     * 按照特定单位时间（秒/分钟/小时/etc）返回平均值
     * @param time
     * @return
     */
    public double getAverageIn(Time time) {
        return this.average == 0.0 ? this.average :
                this.average / time.getTime();
    }
    /**
     * 返回特定时间度量内调用mark的频率
     * @param time
     * @return
     */
    public double getAverageRatePer(Time time) {
        return this.average == 0.0 ? this.average :
                time.getTime() / this.average;
    }
}
