import {z} from 'zod';

import type {QMapToolContext} from '../context/tool-context';

export function createWordCloudTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getDatasetIndexes, WordCloudToolComponent} = ctx;
  return {
    description: 'Render a word cloud from a text field by token frequency.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      textFieldName: z.string().describe('Text field used to extract words'),
      maxWords: z.number().min(5).max(200).optional().describe('Top words to render, default 60'),
      minWordLength: z.number().min(1).max(20).optional().describe('Ignore words shorter than this, default 3'),
      stopWords: z.array(z.string()).optional().describe('Additional stopwords to ignore')
    }),
    execute: async ({datasetName, textFieldName, maxWords, minWordLength, stopWords}: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }
      const resolvedField = resolveDatasetFieldName(dataset, textFieldName);
      if (!resolvedField) {
        return {llmResult: {success: false, details: `Field "${textFieldName}" not found in "${datasetName}".`}};
      }

      const idx = getDatasetIndexes(dataset).slice(0, 120000);
      const minLen = Math.max(1, Number(minWordLength || 3));
      const top = Math.max(5, Number(maxWords || 60));
      const baseStopWords = new Set(
        [
          'the',
          'and',
          'for',
          'with',
          'that',
          'this',
          'from',
          'are',
          'was',
          'have',
          'has',
          'you',
          'your',
          'dei',
          'delle',
          'della',
          'del',
          'dell',
          'per',
          'con',
          'una',
          'uno',
          'che',
          'non',
          'nel',
          'nei',
          'sul',
          'sui',
          'all',
          'alla',
          'alle',
          'gli',
          'dei',
          'dai'
        ].map(v => v.toLowerCase())
      );
      (stopWords || []).forEach((w: string) => {
        const token = String(w || '').trim().toLowerCase();
        if (token) baseStopWords.add(token);
      });

      const freq = new Map<string, number>();
      idx.forEach((rowIdx: number) => {
        const raw = dataset.getValue(resolvedField, rowIdx);
        const text = String(raw ?? '').toLowerCase();
        if (!text) return;
        const tokens = text.split(/[^a-z0-9]+/gi).filter(Boolean);
        tokens.forEach(token => {
          const t = String(token || '').trim().toLowerCase();
          if (!t || t.length < minLen) return;
          if (baseStopWords.has(t)) return;
          freq.set(t, (freq.get(t) || 0) + 1);
        });
      });
      const sorted = Array.from(freq.entries()).sort((a, b) => b[1] - a[1]).slice(0, top);
      if (!sorted.length) {
        return {llmResult: {success: false, details: 'No words found after filtering.'}};
      }
      const maxFreq = Math.max(...sorted.map(([, n]) => n), 1);
      const minFreq = Math.min(...sorted.map(([, n]) => n), maxFreq);
      const palette = ['#0f172a', '#1d4ed8', '#0f766e', '#be123c', '#9333ea', '#b45309'];
      const words = sorted.map(([text, value], i) => {
        const t = maxFreq === minFreq ? 0 : (value - minFreq) / (maxFreq - minFreq);
        const size = Math.round(12 + t * 24);
        return {text, value, size, color: palette[i % palette.length]};
      });
      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          field: resolvedField,
          wordsCount: words.length,
          details: `Rendered word cloud with ${words.length} words from "${resolvedField}".`
        },
        additionalData: {
          title: `Word Cloud - ${dataset.label || dataset.id} / ${resolvedField}`,
          datasetName: dataset.label || dataset.id,
          fieldName: resolvedField,
          words
        }
      };
    },
    component: WordCloudToolComponent as any
  };
}

export function createCategoryBarsTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getDatasetIndexes, CategoryBarsToolComponent} = ctx;
  return {
    description: 'Render top categories as bars for a categorical/text field.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      categoryFieldName: z.string().describe('Categorical field name'),
      topN: z.number().min(3).max(100).optional().describe('Number of categories, default 20')
    }),
    execute: async ({datasetName, categoryFieldName, topN}: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }
      const resolvedField = resolveDatasetFieldName(dataset, categoryFieldName);
      if (!resolvedField) {
        return {
          llmResult: {
            success: false,
            details: `Field "${categoryFieldName}" not found in "${datasetName}".`
          }
        };
      }

      const counts = new Map<string, number>();
      getDatasetIndexes(dataset).forEach((rowIdx: number) => {
        const raw = dataset.getValue(resolvedField, rowIdx);
        const key = String(raw ?? '').trim();
        if (!key) return;
        counts.set(key, (counts.get(key) || 0) + 1);
      });
      const items = Array.from(counts.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, Math.max(3, Number(topN || 20)))
        .map(([label, value]) => ({label, value}));
      if (!items.length) {
        return {llmResult: {success: false, details: 'No categories found for selected field.'}};
      }
      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          field: resolvedField,
          categories: items.length,
          details: `Rendered top ${items.length} categories from "${resolvedField}".`
        },
        additionalData: {
          title: `Top Categories - ${dataset.label || dataset.id} / ${resolvedField}`,
          datasetName: dataset.label || dataset.id,
          fieldName: resolvedField,
          items
        }
      };
    },
    component: CategoryBarsToolComponent as any
  };
}

export function createGrammarAnalyzeTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getDatasetIndexes} = ctx;
  return {
    description:
      'Deterministic frontend text analysis (tokenization, sentence split, token frequencies, optional bigrams) for a dataset text field.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      textFieldName: z.string().describe('Text field to analyze'),
      language: z
        .enum(['it', 'en'])
        .optional()
        .describe('Language hint for tokenization/stopwords, default it'),
      maxRows: z.number().min(1).max(200000).optional().describe('Rows to sample, default 50000'),
      topN: z.number().min(5).max(200).optional().describe('Top tokens to return, default 30'),
      minTokenLength: z.number().min(1).max(20).optional().describe('Ignore shorter tokens, default 2'),
      includeBigrams: z.boolean().optional().describe('Include top bigrams, default true')
    }),
    execute: async ({datasetName, textFieldName, language, maxRows, topN, minTokenLength, includeBigrams}: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }
      const resolvedField = resolveDatasetFieldName(dataset, textFieldName);
      if (!resolvedField) {
        return {llmResult: {success: false, details: `Field "${textFieldName}" not found in "${datasetName}".`}};
      }

      const lang = String(language || 'it').toLowerCase().startsWith('en') ? 'en' : 'it';
      const rowLimit = Math.max(1, Number(maxRows || 50000));
      const tokenLimit = Math.max(5, Number(topN || 30));
      const minLen = Math.max(1, Number(minTokenLength || 2));
      const wantBigrams = includeBigrams !== false;
      const rows = getDatasetIndexes(dataset).slice(0, rowLimit);

      const stopWordsIt = new Set([
        'a',
        'ad',
        'al',
        'alla',
        'alle',
        'allo',
        'ai',
        'agli',
        'all',
        'con',
        'col',
        'da',
        'dal',
        'dalla',
        'dalle',
        'dello',
        'dei',
        'degli',
        'dell',
        'del',
        'di',
        'e',
        'ed',
        'in',
        'il',
        'la',
        'le',
        'lo',
        'gli',
        'i',
        'un',
        'una',
        'uno',
        'su',
        'sul',
        'sui',
        'tra',
        'fra',
        'per',
        'che',
        'chi',
        'non',
        'si'
      ]);
      const stopWordsEn = new Set([
        'a',
        'an',
        'and',
        'are',
        'as',
        'at',
        'be',
        'by',
        'for',
        'from',
        'in',
        'is',
        'it',
        'of',
        'on',
        'or',
        'that',
        'the',
        'to',
        'was',
        'were',
        'with'
      ]);
      const stopWords = lang === 'en' ? stopWordsEn : stopWordsIt;

      const sentenceSegmenter =
        typeof Intl !== 'undefined' && (Intl as any).Segmenter
          ? new Intl.Segmenter(lang, {granularity: 'sentence'})
          : null;
      const wordSegmenter =
        typeof Intl !== 'undefined' && (Intl as any).Segmenter
          ? new Intl.Segmenter(lang, {granularity: 'word'})
          : null;

      let rowWithText = 0;
      let sentenceCount = 0;
      let tokenCount = 0;
      let uniqueTokenCount = 0;
      let totalCharCount = 0;
      let alphaTokenCount = 0;

      const tokenFreq = new Map<string, number>();
      const bigramFreq = new Map<string, number>();

      const addToken = (tokenRaw: string) => {
        const token = String(tokenRaw || '').trim().toLowerCase();
        if (!token || token.length < minLen) return null;
        if (!/[\p{L}\p{N}]/u.test(token)) return null;
        if (stopWords.has(token)) return null;
        tokenFreq.set(token, (tokenFreq.get(token) || 0) + 1);
        tokenCount += 1;
        if (/^\p{L}+$/u.test(token)) {
          alphaTokenCount += 1;
        }
        return token;
      };

      rows.forEach((rowIdx: number) => {
        const raw = dataset.getValue(resolvedField, rowIdx);
        const text = String(raw ?? '').trim();
        if (!text) return;
        rowWithText += 1;
        totalCharCount += text.length;

        if (sentenceSegmenter) {
          sentenceCount += Array.from(sentenceSegmenter.segment(text)).filter(Boolean).length;
        } else {
          const chunks = text.split(/[.!?]+/g).map(s => s.trim()).filter(Boolean);
          sentenceCount += chunks.length || 1;
        }

        const rowTokens: string[] = [];
        if (wordSegmenter) {
          for (const chunk of wordSegmenter.segment(text) as any) {
            if (!chunk?.isWordLike) continue;
            const normalized = addToken(String(chunk.segment || ''));
            if (normalized) rowTokens.push(normalized);
          }
        } else {
          text
            .split(/[^\p{L}\p{N}]+/u)
            .filter(Boolean)
            .forEach(t => {
              const normalized = addToken(t);
              if (normalized) rowTokens.push(normalized);
            });
        }

        if (wantBigrams && rowTokens.length > 1) {
          for (let i = 0; i < rowTokens.length - 1; i += 1) {
            const bigram = `${rowTokens[i]} ${rowTokens[i + 1]}`;
            bigramFreq.set(bigram, (bigramFreq.get(bigram) || 0) + 1);
          }
        }
      });

      uniqueTokenCount = tokenFreq.size;
      if (!rowWithText) {
        return {llmResult: {success: false, details: 'No non-empty text rows found in selected field.'}};
      }

      const topTokens = Array.from(tokenFreq.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, tokenLimit)
        .map(([token, count]) => ({token, count}));
      const topBigrams = wantBigrams
        ? Array.from(bigramFreq.entries())
            .sort((a, b) => b[1] - a[1])
            .slice(0, Math.min(20, tokenLimit))
            .map(([bigram, count]) => ({bigram, count}))
        : [];

      const avgTokensPerSentence = sentenceCount > 0 ? tokenCount / sentenceCount : 0;
      const avgCharsPerRow = rowWithText > 0 ? totalCharCount / rowWithText : 0;
      const lexicalDiversity = tokenCount > 0 ? uniqueTokenCount / tokenCount : 0;
      const alphaRatio = tokenCount > 0 ? alphaTokenCount / tokenCount : 0;

      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          field: resolvedField,
          language: lang,
          rowsScanned: rows.length,
          rowsWithText: rowWithText,
          sentenceCount,
          tokenCount,
          uniqueTokenCount,
          lexicalDiversity: Number(lexicalDiversity.toFixed(6)),
          alphaTokenRatio: Number(alphaRatio.toFixed(6)),
          avgTokensPerSentence: Number(avgTokensPerSentence.toFixed(4)),
          avgCharsPerRow: Number(avgCharsPerRow.toFixed(2)),
          topTokens,
          topBigrams,
          details: `Analyzed "${resolvedField}" with deterministic tokenization over ${rowWithText} text rows.`
        }
      };
    }
  };
}
