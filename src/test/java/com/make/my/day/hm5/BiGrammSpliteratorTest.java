package com.make.my.day.hm5;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

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

        /**
         * Read about bi and n-grams https://en.wikipedia.org/wiki/N-gram.
         *
         * @param source
         */
        public BigrammSpliterator(List<String> source, String delimeter) {
        }

        @Override
        public boolean tryAdvance(Consumer<? super String> action) {
            return false;
        }

        @Override
        public BigrammSpliterator trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return 0;
        }

        @Override
        public int characteristics() {
            return 0;
        }
    }


}
