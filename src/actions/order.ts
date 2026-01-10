'use server'

import { auth } from "@/lib/auth"
import { queryOrderStatus } from "@/lib/epay"
import { processOrderFulfillment } from "@/lib/order-processing"
import { revalidatePath } from "next/cache"
import { db } from "@/lib/db"
import { orders } from "@/lib/db/schema"
import { eq } from "drizzle-orm"

export async function checkOrderStatus(orderId: string) {
    const session = await auth()
    if (!session?.user) return { success: false, error: 'Unauthorized' }

    // Check ownership
    const order = await db.query.orders.findFirst({
        where: eq(orders.orderId, orderId),
        columns: { userId: true, status: true, amount: true, currentPaymentId: true }
    })

    if (!order) return { success: false, error: 'Order not found' }
    if (order.status === 'paid' || order.status === 'delivered') {
        return { success: true, status: order.status }
    }

    // Allow checking if user owns it OR if they have the pending cookie
    if (order.userId && order.userId !== session.user.id) {
        return { success: false, error: 'Unauthorized' }
    }

    try {
        // Use the latest payment ID (retry ID) if available, otherwise fallback to orderId
        const tradeNoToCheck = order.currentPaymentId || orderId
        const result = await queryOrderStatus(tradeNoToCheck)

        if (result.success && result.status === 1) { // 1 = Paid
            // trade_no might be in result.data or result.trade_no?
            // queryOrderStatus returns { ..., data: fullResponse }

            const tradeNo = result.data?.trade_no || result.data?.transaction_id || `MANUAL_CHECK_${Date.now()}`
            const paidAmount = parseFloat(result.data?.money || order.amount)

            await processOrderFulfillment(orderId, paidAmount, tradeNo)

            revalidatePath(`/order/${orderId}`)
            return { success: true, status: 'paid' } // or 'delivered' implicitly via revalidate
        }

        return { success: false, status: 'pending' }

    } catch (e: any) {
        console.error("Check order status failed", e)
        return { success: false, error: e.message }
    }
}
