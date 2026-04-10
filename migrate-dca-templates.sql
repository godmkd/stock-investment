-- DCA Templates table for stock-investment
-- Run in Supabase SQL Editor

CREATE TABLE IF NOT EXISTS public.inv_dca_templates (
  id TEXT PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  data JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE public.inv_dca_templates ENABLE ROW LEVEL SECURITY;

CREATE POLICY "inv_dca_templates_user" ON public.inv_dca_templates
  FOR ALL TO authenticated USING (user_id = auth.uid()) WITH CHECK (user_id = auth.uid());
