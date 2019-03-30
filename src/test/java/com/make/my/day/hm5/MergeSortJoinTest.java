package com.make.my.day.hm5;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javafx.util.Pair;
import org.junit.Test;

public class MergeSortJoinTest {
    @Test
    public void spliteratorTest() {
        List<String> listLeft = Arrays.asList("a b c c o f g h k l".split(" "));
        Collections.shuffle(listLeft);
        Stream<String> left = listLeft.stream();
        List<String> listRight = Arrays.asList("aa bb cc ca cb cd ce dd pp ee ff gg hh kk".split(" "));
        Collections.shuffle(listRight);
        Stream<String> right = listRight.stream();

        List<String> result = StreamSupport.stream(new MergeSortInnerJoinSpliterator<>(left,
                right, Function.identity(), s -> s.substring(0, 1), false), false)
                .map(pair -> pair.getKey() + " " + pair.getValue())
                .collect(Collectors.toList());
        List<String> expected = Stream.of(
                "a aa",
                "b bb",
                "c cc",
                "c ca",
                "c cb",
                "c cd",
                "c ce",
                "c cc",
                "c ca",
                "c cb",
                "c cd",
                "c ce",
                "f ff",
                "g gg",
                "h hh",
                "k kk"
        ).collect(Collectors.toList());

        assertThat("Incorrect result", new HashSet<>(result), is(new HashSet<>(expected)));
        assertThat("Incorrect result order",
                result.stream()
                        .map(s -> s.substring(0,3))
                        .collect(Collectors.toList()),
                is(expected.stream()
                        .map(s -> s.substring(0,3))
                        .collect(Collectors.toList()))
                );
    }

    @Test
    public void spliteratorIntTest() {
        Stream<Integer> left = IntStream.iterate(1, i -> i + 1).limit(10).boxed();
        Stream<String> right = Arrays.stream("0x 1a 2b 3c 4e 5g 9l".split(" "));

        List<String> result = StreamSupport.stream(new MergeSortInnerJoinSpliterator<>(left,
                right, String::valueOf, s -> s.substring(0, 1), false), false)
                .map(pair -> pair.getKey() + " " + pair.getValue())
                .collect(Collectors.toList());
        List<String> expected = Arrays.asList(
                "1 1a",
                "2 2b",
                "3 3c",
                "4 4e",
                "5 5g",
                "9 9l"
        );

        assertThat("Incorrect result", result, is(expected));
    }


    @Test
    public void spliteratorMemoryTest() {
        Stream<Integer> left = IntStream.iterate(1, i -> i + 1).limit(Integer.MAX_VALUE >> 2).boxed();
        Stream<Integer> right = IntStream.iterate(1, i -> i + 1).limit(Integer.MAX_VALUE >> 2).boxed();

        long count = StreamSupport.stream(new MergeSortInnerJoinSpliterator<>(left,
                right, Function.identity(), Function.identity(), true), false)
                .count();
        assertThat("Incorrect result", count, is((long)Integer.MAX_VALUE >> 2));
    }

    //ToDo: Implement your own merge sort inner join spliterator. See https://en.wikipedia.org/wiki/Sort-merge_join
    public static class MergeSortInnerJoinSpliterator<C extends Comparable<C>, L, R> implements Spliterator<Pair<L, R>> {
        private Iterator<L> left;
        private Iterator<R> right;
        private Function<L, C> keyExtractorLeft;
        private Function<R, C> keyExtractorRight;
        private int indexL = 0;
        private int indexR = 0;
        private L nextL;
        private R nextR;
        private List<L> leftBuffer = new ArrayList<>();
        private List<R> rightBuffer = new ArrayList<>();

        public MergeSortInnerJoinSpliterator(Stream<L> left,
                                             Stream<R> right,
                                             Function<L, C> keyExtractorLeft,
                                             Function<R, C> keyExtractorRight,
                                             boolean isSorted) {
            if (!isSorted) {
                left = left.sorted(Comparator.comparing(keyExtractorLeft));
                right = right.sorted(Comparator.comparing(keyExtractorRight));
            }
            this.left = left.iterator();
            this.right = right.iterator();
            this.keyExtractorLeft = keyExtractorLeft;
            this.keyExtractorRight = keyExtractorRight;
            if (this.left.hasNext() && this.right.hasNext()) {
                nextL = this.left.next();
                nextR = this.right.next();
            }
        }

        @Override
        public boolean tryAdvance(Consumer<? super Pair<L, R>> action) {
            if (leftBuffer.isEmpty()) {
                fillBuffers();
            }
            if (leftBuffer.size() > 0 && rightBuffer.size() > 0) {
                for (; indexL < leftBuffer.size(); indexL++) {
                    L l = leftBuffer.get(indexL);
                    for (;indexR < rightBuffer.size(); indexR++) {
                        R r = rightBuffer.get(indexR);
                        if (keyExtractorLeft.apply(l).equals(keyExtractorRight.apply(r))) {
                            action.accept(new Pair<>(l,r));
                            indexR++;
                            return true;
                        }
                    }
                    indexR = 0;
                    if (indexL == leftBuffer.size() - 1) {
                        fillBuffers();
                        indexL--;
                    }
                }
            }
            return false;
        }

        private void fillBuffers() {
            boolean severalInARow = false;
            leftBuffer = new ArrayList<>();
            rightBuffer = new ArrayList<>();
            while (leftBuffer.size() < 1 || severalInARow) {
                if (nextL == null) break;
                leftBuffer.add(nextL);
                nextL = left.hasNext() ? left.next() : null;
                severalInARow = keyExtractorLeft.apply(leftBuffer.get(leftBuffer.size()-1))
                    .equals(keyExtractorLeft.apply(nextL));
            }
            if (!leftBuffer.isEmpty()) {
                while (nextR != null && keyExtractorLeft.apply(leftBuffer.get(leftBuffer.size() - 1))
                    .compareTo(keyExtractorRight.apply(nextR)) >= 0) {
                    rightBuffer.add(nextR);
                    nextR = right.hasNext() ? right.next() : null;
                }
            }
            indexL = 0;
            indexR = 0;
        }

        @Override
        public Spliterator<Pair<L, R>> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return ORDERED | IMMUTABLE;
        }
    }

}
