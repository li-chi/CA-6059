/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.memory;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.codahale.metrics.Timer;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.service.GCInspectorMXBean;
import java.io.*;
import java.util.*;
import static java.lang.Math.sqrt;
import java.lang.management.ManagementFactory;

/**
 * Represents an amount of memory used for a given purpose, that can be allocated to specific tasks through
 * child MemtableAllocator objects.
 */
public abstract class MemtablePool
{
    final MemtableCleanerThread<?> cleaner;

    // the total memory used by this pool
    public final SubPool onHeap;
    public final SubPool offHeap;

    public final Timer blockedOnAllocating;

    final WaitQueue hasRoom = new WaitQueue();



    MemtablePool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanThreshold, Runnable cleaner) throws Exception
    {

        this.onHeap = getSubPool(maxOnHeapMemory, cleanThreshold);
        this.offHeap = getSubPool(maxOffHeapMemory, cleanThreshold);
        this.cleaner = getCleaner(cleaner);
        blockedOnAllocating = CassandraMetricsRegistry.Metrics.timer(new DefaultNameFactory("MemtablePool")
                                                                         .createMetricName("BlockedOnAllocation"));
        if (this.cleaner != null)
            this.cleaner.start();
        try
        {
            GCInspector.register(onHeap);
        }
        catch (Throwable t)
        {
        }
    }


    SubPool getSubPool(long limit, float cleanThreshold) throws Exception
    {
        return new SubPool(limit, cleanThreshold);
    }

    MemtableCleanerThread<?> getCleaner(Runnable cleaner)
    {
        return cleaner == null ? null : new MemtableCleanerThread<>(this, cleaner);
    }

    public abstract MemtableAllocator newAllocator();

    /**
     * Note the difference between acquire() and allocate(); allocate() makes more resources available to all owners,
     * and acquire() makes shared resources unavailable but still recorded. An Owner must always acquire resources,
     * but only needs to allocate if there are none already available. This distinction is not always meaningful.
     */
/*
    class MemMonitor2 implements Runnable {
        public void run() {
            Runtime runtime = Runtime.getRuntime(); 
            String fullpid=ManagementFactory.getRuntimeMXBean().getName(); 
            String[] parts = fullpid.split("\\@"); 
            String pid = parts[0]; 
            while(true){
                pw.println(System.nanoTime()+","+runtime.totalMemory()/1048576+","+runtime.freeMemory()/1048576+","+runtime.maxMemory()/1048576);
                try {
                    Thread.sleep(100);
                } 
                catch(InterruptedException e) {
                }
            }    
        }
    } 
*/
    public class SubPool
    {

        // total memory/resource permitted to allocate
        public long limit;

        // ratio of used to spare (both excluding 'reclaiming') at which to trigger a clean
        public final float cleanThreshold;

        // total bytes allocated and reclaiming
        volatile long allocated;
        volatile long reclaiming;

        // a cache of the calculation determining at what allocation threshold we should next clean
        volatile long nextClean;
        private long usedMem = -1;
        private long heapSize = Runtime.getRuntime().maxMemory();
        private long lowest = Runtime.getRuntime().maxMemory() / 9;
        private long highest = Runtime.getRuntime().maxMemory() / 3;
        private long[] x;
        private long[][] y;
        private int length = 8;
        private int stage;
        private int substage;
        private PrintWriter pw = new PrintWriter(new FileOutputStream(new File("d_memory.log"),true));
        private int count = 0;
        private long current = 0;
        private long WARM_UP = 300000000;
        private long PERIOD = 50000000;
        private double GOAL;
        private double P;
        private double defaultP = 0.621;
        private long defaultLimit;
        private double alpha = 0.657;
        private double lambda = 0.113;
        private String fullpid=ManagementFactory.getRuntimeMXBean().getName();
        private double ms[];
        private long[][] new_x;

        public SubPool(long limit, float cleanThreshold) throws Exception
        {
            this.limit = defaultLimit = limit;
            this.cleanThreshold = cleanThreshold;
            x = new long[length];
            y = new long[length][length];
            new_x = new long[length][length];
            long step = (highest - lowest) / (length-1);
            for (int i=0; i<length; i++) {
                x[i] = lowest + i * step;
            }
            setLimit(x[0]);
            
            stage = -1;

            GOAL = (1.0-lambda) * (double)heapSize;
            PERIOD = 10000000;
            pw.println("alpha,"+alpha+",lambda,"+lambda+",GOAL,"+(long)GOAL+",heap,"+heapSize);
            pw.flush();
        }

        double getMean(long[] a) {
            long sum = 0;
            for (int i=0; i<a.length; i++) {
                sum += a[i];
            }
            return (double)sum / (double)a.length;
        }

        double getMean(double[] a) {
            double sum = 0;
            for (int i=0; i<a.length; i++) {
                sum += a[i];
            }
            return sum / (double)a.length;
        }

        double getStand(long[] a, double m) {
            double sum = 0;
            for (int i=0; i<a.length; i++) {
                sum += ((double)a[i]-m)*((double)a[i]-m);
            }
            return Math.sqrt(sum);
        }

        double getStand(double[] a, double m) {
            double sum = 0;
            for (int i=0; i<a.length; i++) {
                sum += (a[i]-m)*(a[i]-m);
            }
            return Math.sqrt(sum);
        }

        double getMin(double a[]) {
            double min = Double.MAX_VALUE;
            for (int i=0; i<a.length; i++) {
                if (a[i] < min) {
                    min = a[i];
                }
            }
            return min;
        }

        private void computeP() {
            double[] yy = new double[length];
            double ymin = getMin(ms);
            for (int i=0; i<ms.length; i++) {
                yy[i] = ms[i] - ymin;
            }
            double stand = getStand(yy);
            double delta = stand / getMean(yy);
            defaultP = 1.0 - 2.0 / delta / 3.0;
            if (defaultP < 0 || P > 1) {
                defaultP = 0.5;
            }
            pw.println("P:"+defaultP+" delta:"+delta);
            pw.flush();
        }

        double getStand(double[] a) {
            return getStand(a, getMean(a));
        }

        void computeLimit() {
            if(usedMem > GOAL) {
                P = 0.0;
            } else {
                P = defaultP;
            }
            limit = allocated + (long)((1-P)/alpha * (GOAL - (double)usedMem));
            if (limit < defaultLimit/2) {
                limit = defaultLimit/2;
            }
            pw.println("data,"+usedMem+","+limit+","+allocated+","+System.nanoTime());
            pw.flush();
        }

        void computePara() {
            ms = new double[length];
            double[] stands = new double[length];
            double []mx = new double[length];
            for (int i=0; i<length; i++) {
                mx[i] = getMean(new_x[i]);
                ms[i] = getMean(y[i]);
                stands[i] = getStand(y[i],ms[i]);
            }

            double[] xx = new double[length];
            for (int i=0; i<length; i++) {
                xx[i] = (double)mx[i];
            }
            LinearRegression lr = new LinearRegression(xx,ms);
            alpha = lr.slope();

            double sum = 0;
            for (int i=0; i<length; i++) {
                sum += (stands[i] / ms[i]);
            }
            lambda = sum / length;
            GOAL = (1.0-lambda) * (double)heapSize;
            PERIOD = 10000000;
            pw.println("alpha,"+alpha+",lambda,"+lambda+",GOAL,"+(long)GOAL+",heap,"+heapSize);
            pw.flush();
        }

        /** Methods to allocate space **/
        boolean tryAllocate(long size)
        {   
            //pw.println(size);
            current += size;
            if (current >= PERIOD) {
                //pw.println(fullpid+","+stage+","+substage+","+usedMem+","+current+","+limit);
                //pw.flush();
                if (stage > -1) {
                    current = 0;
                }
                if (stage == -1) {
                    if (current >= WARM_UP) {
                        stage++;
                        current = 0;
                    }
                } else if (stage < length-1) {
                    // profiling
                    new_x[stage][substage] = limit;
                    y[stage][substage++] = usedMem;
                    if (substage >= length) {
                        setLimit(x[++stage]);
                        substage = 0;
                    }
                } else if (stage == length-1) {
                    new_x[stage][substage] = limit;
                    y[stage][substage++] = usedMem;
                    if (substage >= length) {
                        setLimit(defaultLimit);
                        substage = 0;
                        stage++;
                        computePara();
                        computeP();
                    }
                } else {
                    computeLimit();
                }
            }
            
            while (true)
            {
                long cur;
                if ((cur = allocated) + size > limit)
                    return false;
                if (allocatedUpdater.compareAndSet(this, cur, cur + size))
                    return true;
            }
        }

        public void setLimit(long l) {
            this.limit = l;
        }

        public long getLimit() {
            return this.limit;
        }

        public void setCurrentUsedMem(long mem) {
            this.usedMem = mem;
        }

        /** Methods for tracking and triggering a clean **/

        boolean needsCleaning()
        {
            // use strictly-greater-than so we don't clean when limit is 0
            return used() > nextClean && updateNextClean();
        }

        void maybeClean()
        {
            if (needsCleaning() && cleaner != null)
                cleaner.trigger();
        }

        private boolean updateNextClean()
        {
            while (true)
            {
                long current = nextClean;
                long reclaiming = this.reclaiming;
                long next =  reclaiming + (long) (this.limit * cleanThreshold);
                if (current == next || nextCleanUpdater.compareAndSet(this, current, next))
                    return used() > next;
            }
        }

        /**
         * apply the size adjustment to allocated, bypassing any limits or constraints. If this reduces the
         * allocated total, we will signal waiters
         */
        private void adjustAllocated(long size)
        {
            while (true)
            {
                long cur = allocated;
                if (allocatedUpdater.compareAndSet(this, cur, cur + size))
                    return;
            }
        }

        void allocated(long size)
        {
            assert size >= 0;
            if (size == 0)
                return;

            adjustAllocated(size);
            maybeClean();
        }

        void acquired(long size)
        {
            maybeClean();
        }

        void released(long size)
        {
            assert size >= 0;
            adjustAllocated(-size);
            hasRoom.signalAll();
        }

        void reclaiming(long size)
        {
            if (size == 0)
                return;
            reclaimingUpdater.addAndGet(this, size);
        }

        void reclaimed(long size)
        {
            if (size == 0)
                return;

            reclaimingUpdater.addAndGet(this, -size);
            if (updateNextClean() && cleaner != null)
                cleaner.trigger();
        }

        public long used()
        {
            return allocated;
        }

        public float reclaimingRatio()
        {
            float r = reclaiming / (float) limit;
            if (Float.isNaN(r))
                return 0;
            return r;
        }

        public float usedRatio()
        {
            float r = allocated / (float) limit;
            if (Float.isNaN(r))
                return 0;
            return r;
        }

        public long usedNum() {
            return allocated;
        }

        public MemtableAllocator.SubAllocator newAllocator()
        {
            return new MemtableAllocator.SubAllocator(this);
        }

        public WaitQueue hasRoom()
        {
            return hasRoom;
        }

        public Timer.Context blockedTimerContext()
        {
            return blockedOnAllocating.time();
        }
    }

    private static final AtomicLongFieldUpdater<SubPool> reclaimingUpdater = AtomicLongFieldUpdater.newUpdater(SubPool.class, "reclaiming");
    private static final AtomicLongFieldUpdater<SubPool> allocatedUpdater = AtomicLongFieldUpdater.newUpdater(SubPool.class, "allocated");
    private static final AtomicLongFieldUpdater<SubPool> nextCleanUpdater = AtomicLongFieldUpdater.newUpdater(SubPool.class, "nextClean");

}

class LinearRegression {
    private final double intercept, slope;
    private final double r2;
    private final double svar0, svar1;

   /**
     * Performs a linear regression on the data points {@code (y[i], x[i])}.
     *
     * @param  x the values of the predictor variable
     * @param  y the corresponding values of the response variable
     * @throws IllegalArgumentException if the lengths of the two arrays are not equal
     */
    public LinearRegression(double[] x, double[] y) {
        if (x.length != y.length) {
            throw new IllegalArgumentException("array lengths are not equal");
        }
        int n = x.length;

        // first pass
        double sumx = 0.0, sumy = 0.0, sumx2 = 0.0;
        for (int i = 0; i < n; i++) {
            sumx  += x[i];
            sumx2 += x[i]*x[i];
            sumy  += y[i];
        }
        double xbar = sumx / n;
        double ybar = sumy / n;

        // second pass: compute summary statistics
        double xxbar = 0.0, yybar = 0.0, xybar = 0.0;
        for (int i = 0; i < n; i++) {
            xxbar += (x[i] - xbar) * (x[i] - xbar);
            yybar += (y[i] - ybar) * (y[i] - ybar);
            xybar += (x[i] - xbar) * (y[i] - ybar);
        }
        slope  = xybar / xxbar;
        intercept = ybar - slope * xbar;

        // more statistical analysis
        double rss = 0.0;      // residual sum of squares
        double ssr = 0.0;      // regression sum of squares
        for (int i = 0; i < n; i++) {
            double fit = slope*x[i] + intercept;
            rss += (fit - y[i]) * (fit - y[i]);
            ssr += (fit - ybar) * (fit - ybar);
        }

        int degreesOfFreedom = n-2;
        r2    = ssr / yybar;
        double svar  = rss / degreesOfFreedom;
        svar1 = svar / xxbar;
        svar0 = svar/n + xbar*xbar*svar1;
    }

   /**
     * Returns the <em>y</em>-intercept &alpha; of the best of the best-fit line <em>y</em> = &alpha; + &beta; <em>x</em>.
     *
     * @return the <em>y</em>-intercept &alpha; of the best-fit line <em>y = &alpha; + &beta; x</em>
     */
    public double intercept() {
        return intercept;
    }

   /**
     * Returns the slope &beta; of the best of the best-fit line <em>y</em> = &alpha; + &beta; <em>x</em>.
     *
     * @return the slope &beta; of the best-fit line <em>y</em> = &alpha; + &beta; <em>x</em>
     */
    public double slope() {
        return slope;
    }

   /**
     * Returns the coefficient of determination <em>R</em><sup>2</sup>.
     *
     * @return the coefficient of determination <em>R</em><sup>2</sup>,
     *         which is a real number between 0 and 1
     */
    public double R2() {
        return r2;
    }

   /**
     * Returns the standard error of the estimate for the intercept.
     *
     * @return the standard error of the estimate for the intercept
     */
    public double interceptStdErr() {
        return Math.sqrt(svar0);
    }

   /**
     * Returns the standard error of the estimate for the slope.
     *
     * @return the standard error of the estimate for the slope
     */
    public double slopeStdErr() {
        return Math.sqrt(svar1);
    }

   /**
     * Returns the expected response {@code y} given the value of the predictor
     * variable {@code x}.
     *
     * @param  x the value of the predictor variable
     * @return the expected response {@code y} given the value of the predictor
     *         variable {@code x}
     */
    public double predict(double x) {
        return slope*x + intercept;
    }

   /**
     * Returns a string representation of the simple linear regression model.
     *
     * @return a string representation of the simple linear regression model,
     *         including the best-fit line and the coefficient of determination
     *         <em>R</em><sup>2</sup>
     */
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(String.format("%.2f n + %.2f", slope(), intercept()));
        s.append("  (R^2 = " + String.format("%.3f", R2()) + ")");
        return s.toString();
    }

}

