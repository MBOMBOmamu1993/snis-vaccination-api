"use client";

import { create } from "zustand";
import type { Filters } from "./calc";

interface FilterState extends Filters {
  set: (patch: Partial<Filters>) => void;
  reset: () => void;
}

const EMPTY: Filters = { province: "", antenne: "", zs: "", months: [] };

export const useFilters = create<FilterState>((set) => ({
  ...EMPTY,
  set: (patch) => set((s) => ({ ...s, ...patch })),
  reset: () => set((s) => ({ ...s, ...EMPTY })),
}));
