import * as DialogPrimitive from "@radix-ui/react-dialog";
import { Slot } from "@radix-ui/react-slot";
import * as ToastPrimitive from "@radix-ui/react-toast";
import { cva, type VariantProps } from "class-variance-authority";
import { X } from "lucide-react";
import {
  createContext,
  forwardRef,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ButtonHTMLAttributes,
  type HTMLAttributes,
  type InputHTMLAttributes,
  type ReactNode
} from "react";
import { twMerge } from "tailwind-merge";
import clsx, { type ClassValue } from "clsx";

export function cn(...values: ClassValue[]): string {
  return twMerge(clsx(values));
}

const buttonVariants = cva(
  "inline-flex min-h-[var(--control-height)] items-center justify-center gap-[var(--space-2)] rounded-[var(--radius-control)] px-[var(--space-4)] [font-size:var(--text-sm)] leading-[var(--leading-tight)] font-semibold transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--focus)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--surface)] disabled:pointer-events-none disabled:opacity-50",
  {
    variants: {
      variant: {
        primary: "bg-[var(--accent)] text-white hover:bg-[var(--accent-strong)]",
        secondary: "border border-[var(--border)] bg-[var(--surface-raised)] text-[var(--text)] hover:bg-[var(--surface-hover)]",
        danger: "bg-[var(--danger)] text-white hover:brightness-95",
        ghost: "text-[var(--muted)] hover:bg-[var(--surface-hover)] hover:text-[var(--text)]"
      },
      size: {
        default: "h-[var(--control-height)]",
        compact: "h-9 min-h-9 px-[var(--space-3)]",
        icon: "h-10 w-10 px-0"
      }
    },
    defaultVariants: { variant: "primary", size: "default" }
  }
);

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement>, VariantProps<typeof buttonVariants> {
  asChild?: boolean;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  { asChild = false, className, variant, size, ...props },
  ref
) {
  const Component = asChild ? Slot : "button";
  return <Component className={cn(buttonVariants({ variant, size }), className)} ref={ref} {...props} />;
});

export const Card = forwardRef<HTMLDivElement, HTMLAttributes<HTMLDivElement>>(function Card(
  { className, ...props },
  ref
) {
  return <div ref={ref} className={cn("rounded-[var(--radius-panel)] border border-[var(--border)] bg-[var(--surface-raised)] p-[var(--space-5)] [box-shadow:var(--shadow-panel)]", className)} {...props} />;
});

export const Input = forwardRef<HTMLInputElement, InputHTMLAttributes<HTMLInputElement>>(function Input(
  { className, ...props },
  ref
) {
  return (
    <input
      ref={ref}
      className={cn("min-h-[var(--control-height)] w-full rounded-[var(--radius-control)] border border-[var(--border)] bg-[var(--input)] px-[var(--space-3)] [font-size:var(--text-sm)] leading-[var(--leading-normal)] text-[var(--text)] outline-none placeholder:text-[var(--muted)] focus-visible:ring-2 focus-visible:ring-[var(--focus)]", className)}
      {...props}
    />
  );
});

export function Field({ label, error, hint, children }: { label: string; error?: string; hint?: string; children: ReactNode }) {
  return (
    <label className="grid gap-[var(--space-1)] [font-size:var(--text-sm)] leading-[var(--leading-normal)] font-medium text-[var(--text)]">
      <span>{label}</span>
      {children}
      {error ? <span className="[font-size:var(--text-xs)] text-[var(--danger)]" role="alert">{error}</span> : null}
      {!error && hint ? <span className="[font-size:var(--text-xs)] font-normal text-[var(--muted)]">{hint}</span> : null}
    </label>
  );
}

export function Badge({ tone = "neutral", children }: { tone?: "neutral" | "success" | "warning" | "danger"; children: ReactNode }) {
  const tones = {
    neutral: "bg-[var(--surface-hover)] text-[var(--muted)]",
    success: "bg-[var(--success-soft)] text-[var(--success)]",
    warning: "bg-[var(--warning-soft)] text-[var(--warning)]",
    danger: "bg-[var(--danger-soft)] text-[var(--danger)]"
  };
  return <span className={cn("inline-flex items-center rounded-full px-[var(--space-3)] py-[var(--space-1)] [font-size:var(--text-xs)] leading-[var(--leading-tight)] font-semibold", tones[tone])}>{children}</span>;
}

export function Dialog({
  open,
  onOpenChange,
  restoreFocus,
  title,
  description,
  children,
  footer,
  closeLabel,
  closeDisabled = false
}: {
  open: boolean;
  onOpenChange(open: boolean): void;
  restoreFocus?(): void;
  title: string;
  description?: string;
  children: ReactNode;
  footer?: ReactNode;
  closeLabel: string;
  closeDisabled?: boolean;
}) {
  return (
    <DialogPrimitive.Root open={open} onOpenChange={onOpenChange}>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="fixed inset-0 z-40 bg-black/50 backdrop-blur-[2px] data-[state=closed]:animate-none" />
        <DialogPrimitive.Content
          className="fixed left-1/2 top-1/2 z-50 max-h-[90vh] w-[min(92vw,680px)] -translate-x-1/2 -translate-y-1/2 overflow-auto rounded-[var(--radius-panel)] border border-[var(--border)] bg-[var(--surface-raised)] p-[var(--space-6)] text-[var(--text)] shadow-2xl focus:outline-none"
          onCloseAutoFocus={(event) => {
            if (!restoreFocus) return;
            event.preventDefault();
            restoreFocus();
          }}
        >
          <div className="pr-10">
            <DialogPrimitive.Title className="text-xl font-bold">{title}</DialogPrimitive.Title>
            {description ? <DialogPrimitive.Description className="mt-1 text-sm text-[var(--muted)]">{description}</DialogPrimitive.Description> : null}
          </div>
          <DialogPrimitive.Close asChild>
            <Button aria-label={closeLabel} className="absolute right-4 top-4" disabled={closeDisabled} size="icon" type="button" variant="ghost"><X size={18} /></Button>
          </DialogPrimitive.Close>
          <div className="mt-[var(--space-5)]">{children}</div>
          {footer ? <div className="mt-[var(--space-6)] flex flex-wrap justify-end gap-[var(--space-3)]">{footer}</div> : null}
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  );
}

interface ToastItem { id: number; title: string; description?: string; tone: "success" | "warning" | "danger"; }
interface ToastContextValue { push(item: Omit<ToastItem, "id">): void; }
const ToastContext = createContext<ToastContextValue | null>(null);

export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<ToastItem[]>([]);
  const push = useCallback((item: Omit<ToastItem, "id">) => {
    const id = Date.now() + Math.floor(Math.random() * 1000);
    setItems((current) => [...current, { ...item, id }]);
  }, []);
  const value = useMemo(() => ({ push }), [push]);
  return (
    <ToastContext.Provider value={value}>
      <ToastPrimitive.Provider duration={5000} swipeDirection="right">
        {children}
        {items.map((item) => (
          <ToastPrimitive.Root
            className={cn(
              "grid w-[min(92vw,420px)] gap-[var(--space-1)] rounded-xl border bg-[var(--surface-raised)] p-[var(--space-4)] text-[var(--text)] shadow-xl",
              item.tone === "danger" ? "border-[var(--danger)]" : item.tone === "warning" ? "border-[var(--warning)]" : "border-[var(--success)]"
            )}
            key={item.id}
            onOpenChange={(open) => { if (!open) setItems((current) => current.filter((entry) => entry.id !== item.id)); }}
          >
            <ToastPrimitive.Title className="font-semibold">{item.title}</ToastPrimitive.Title>
            {item.description ? <ToastPrimitive.Description className="[font-size:var(--text-sm)] leading-[var(--leading-normal)] text-[var(--muted)]">{item.description}</ToastPrimitive.Description> : null}
          </ToastPrimitive.Root>
        ))}
        <ToastPrimitive.Viewport className="fixed bottom-5 right-5 z-[60] grid gap-[var(--space-3)] outline-none" />
      </ToastPrimitive.Provider>
    </ToastContext.Provider>
  );
}

export function useToast(): ToastContextValue {
  const value = useContext(ToastContext);
  if (!value) throw new Error("useToast must be used inside ToastProvider.");
  return value;
}
