package com.make.my.day.hm5;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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

        Set<String> result = StreamSupport.stream(new MergeSortInnerJoinSpliterator<>(left,
                right, Function.identity(), s -> s.substring(0, 1)), true)
                .map(pair -> pair.getKey() + " " + pair.getValue())
                .collect(Collectors.toSet());
        Set<String> expected = Stream.of(
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
        ).collect(Collectors.toSet());

        assertThat("Incorrect result", result, is(expected));
    }

    @Test
    public void spliteratorIntTest() {
        Stream<Integer> left = IntStream.iterate(1, i -> i + 1).limit(10).boxed();
        Stream<String> right = Arrays.stream("0x 1a 2b 3c 4e 5g 9l".split(" "));

        List<String> result = StreamSupport.stream(new MergeSortInnerJoinSpliterator<>(left,
                right, String::valueOf, s -> s.substring(0, 1)), true)
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

    //ToDo: Implement your own merge sort inner join spliterator. See https://en.wikipedia.org/wiki/Sort-merge_join
    public static class MergeSortInnerJoinSpliterator<C extends Comparable<C>, L, R> implements Spliterator<Pair<L, R>> {
        private List<L> left;
        private List<R> right;
        private Function<L, C> keyEextractorLeft;
        private Function<R, C> keyExtractorRight;

        public MergeSortInnerJoinSpliterator(Stream<L> left,
                                             Stream<R> right,
                                             Function<L, C> keyExtractorLeft,
                                             Function<R, C> keyExtractorRight) {
            this.left = left.collect(Collectors.toList());
            this.right = right.collect(Collectors.toList());
            this.keyEextractorLeft = keyExtractorLeft;
            this.keyExtractorRight = keyExtractorRight;
        }

        @Override
        public boolean tryAdvance(Consumer<? super Pair<L, R>> action) {
            boolean res = false;
            for (L l : left) {
                R t = right.stream().filter(r ->
                    keyEextractorLeft.apply(l).equals(keyExtractorRight.apply(r))).findFirst()
                    .orElse(null);
                if (t != null) {
                    action.accept(new Pair<>(l,t));
                    right.remove(t);
                    res = true;
                    break;
                }
            }
            return res;
        }

        @Override
        public Spliterator<Pair<L, R>> trySplit() {
            MergeSortInnerJoinSpliterator res = null;
            if (left.size() > 1) {
              List<List<L>> halvesLeft = new ArrayList<>(left.stream()
                  .collect(Collectors.partitioningBy(l -> left.indexOf(l) >= left.size() / 2))
                  .values());
              List<R> firstHalfRight = halvesLeft.get(0).stream()
                  .flatMap(l -> right.stream().filter(r ->
                      keyEextractorLeft.apply(l).equals(keyExtractorRight.apply(r))))
                  .collect(Collectors.toList());
              List<R> secondHalfRight = halvesLeft.get(1).stream()
                  .flatMap(l -> right.stream().filter(r ->
                      keyEextractorLeft.apply(l).equals(keyExtractorRight.apply(r))))
                  .collect(Collectors.toList());
              if (firstHalfRight.size() > 0 && secondHalfRight.size() > 0) {
                res = new MergeSortInnerJoinSpliterator(halvesLeft.get(0).stream(),
                    firstHalfRight.stream(),
                    keyEextractorLeft, keyExtractorRight);
                left = halvesLeft.get(1);
                right = secondHalfRight;
              }
            }
            return res;
        }

        @Override
        public long estimateSize() {
            return Integer.max(left.size(),right.size());
        }

        @Override
        public int characteristics() {
            return ORDERED | SIZED | IMMUTABLE | SUBSIZED;
        }
    }

}
