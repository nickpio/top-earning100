export const STOPWORDS = new Set([
    // English filler
    "the","a","an","and","or","of","to","for","in","on","with","by","from",
    "is","are","be","this","that","it","as",
  
    // Generic game fluff
    "game","games","experience","roblox","official",
    "play","playing","fun","best","ultimate","world",
  
    // Release / lifecycle noise
    "upd","update","updated","updates",
    "beta","alpha","test","testing",
    "new","old","v","ver","version","release","patch","hotfix",
    "fix","fixed","fixes", "event",
  
    // Engagement bait
    "join","like","favorite","favorites","group","follow",
    "free","reward","rewards","codes","code",
  
    // Common title spam
    "open","openworld","official",
  ]);
  
  export const SYNONYMS: Record<string, string> = {
    sim: "simulator",
    sims: "simulator",
    tycoons: "tycoon",
    upd: "update", // collapse → filtered anyway
  };