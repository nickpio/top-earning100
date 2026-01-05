export type NgramOptions = {
    minN?: 1 | 2 | 3;
    maxN?: 1 | 2 | 3;
    joiner?: string;            // default " "
    minTokenLen?: number;       // default 3
    allowShortTokens?: Set<string>; // e.g. rng, fps, obby
  };
  
  export function buildNgrams(tokens: string[], opts?: NgramOptions): string[] {
    const minN = opts?.minN ?? 1;
    const maxN = opts?.maxN ?? 3;
    const joiner = opts?.joiner ?? " ";
    const minTokenLen = opts?.minTokenLen ?? 3;
    const allowShort = opts?.allowShortTokens ?? new Set<string>();
  
    // Filter tokens for n-gram building (keep short allowlisted tokens)
    const t = tokens.filter((w) => w.length >= minTokenLen || allowShort.has(w));
    const out: string[] = [];
  
    for (let n = minN; n <= maxN; n++) {
      for (let i = 0; i + n <= t.length; i++) {
        const gram = t.slice(i, i + n).join(joiner);
        out.push(gram);
      }
    }
  
    return out;
  }