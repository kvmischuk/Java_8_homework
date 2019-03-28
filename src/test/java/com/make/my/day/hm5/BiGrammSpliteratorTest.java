package com.make.my.day.hm5;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.Test;

public class BiGrammSpliteratorTest {

    @Test
    public void biGramSplitTest() throws Exception {
        List<String> tokens = Arrays.asList("I should never try to implement my own spliterator".split(" "));

        Set<String> result = StreamSupport.stream(new BigrammSpliterator(tokens, " "), true)
                .collect(Collectors.toSet());

        Set<String> expected = Arrays.stream(new String[]{
                "I should",
                "should never",
                "never try",
                "try to",
                "to implement",
                "implement my",
                "my own",
                "own spliterator"
        }).collect(Collectors.toSet());

        assertThat("Incorrect result", result, is(expected));

    }

    @Test
    public void biGramSplitTestSplit() throws Exception {
        List<String> tokens = Arrays.asList("I should never try to implement my own spliterator".split(" "));

        BigrammSpliterator biGrammSpliterator = new BigrammSpliterator(tokens, " ");
        BigrammSpliterator biGramSpliterator1 = biGrammSpliterator.trySplit();

        assertThat("Spliterator 1 is null", biGramSpliterator1, notNullValue());

        BigrammSpliterator biGramSpliterator2 = biGramSpliterator1.trySplit();

        assertThat("Spliterator 2 is null", biGramSpliterator2, notNullValue());
        Consumer<String> consumer = (String s) -> {
        };
        int count = 0;
        while (biGrammSpliterator.tryAdvance(consumer)) {
            count++;
        }

        assertThat("Incorrect Spliterator 0 size", count, is(4));

        count = 0;
        while (biGramSpliterator1.tryAdvance(consumer)) {
            count++;
        }

        assertThat("Incorrect Spliterator 1 size", count, is(2));

        count = 0;
        while (biGramSpliterator2.tryAdvance(consumer)) {
            count++;
        }

        assertThat("Incorrect Spliterator 2 size", count, is(2));

    }

    class BigrammSpliterator implements Spliterator<String> {
        //ToDo: Write your own bi-gram spliterator
        //Todo: Should works in parallel
        private List<String> source;
        private String delimeter;
        private int index = 0;
        private int last;

        /**
         * Read about bi and n-grams https://en.wikipedia.org/wiki/N-gram.
         *
         * @param source
         */
        public BigrammSpliterator(List<String> source, String delimeter) {
            this.source = source;
            this.delimeter = delimeter;
            last = source.size() - 1;
        }

        @Override
        public boolean tryAdvance(Consumer<? super String> action) {
            boolean res = false;
            if (index <= last - 1) {
                String s = source.get(index) + delimeter + source.get(index + 1);
                action.accept(s);
                index++;
                res = true;
            }
            return res;
        }

        @Override
        public BigrammSpliterator trySplit() {
            BigrammSpliterator result = null;
            if (source.size() > 2) {
                int splitIndex = index + (last - index) / 2;
                List<List<String>> halves = new ArrayList<>(source.stream().
                    collect(Collectors.partitioningBy(i -> source.indexOf(i) > splitIndex)
                    ).values());
                List<String> firstHalf = halves.get(0);
                List<String> secondHalf = halves.get(1);
                String borderString = firstHalf.get(firstHalf.size()-1);
                secondHalf.add(0, borderString);
                result = new BigrammSpliterator(firstHalf, delimeter);
                source = secondHalf;
                index = 0;
                last = source.size() - 1;
            }
            return result;
        }

        @Override
        public long estimateSize() {
            return last - index;
        }

        @Override
        public int characteristics() {
            return ORDERED | SIZED | IMMUTABLE | SUBSIZED;
        }
    }


}
